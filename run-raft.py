import json
import sys
import socket
import logging
import itertools as it
from uuid import uuid4
from time import time, sleep
from random import uniform, seed
from functools import partial
from dataclasses import dataclass

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class Stream(object):
    def __init__(self, head, rest_fn, exit=None):
        self.head = head
        self.exit = exit
        self._rest = None
        self._rest_fn = rest_fn
        self._evaluated = False

    @property
    def stopped(self):
        return self.exit is not None

    @property
    def rest(self):
        assert not self.stopped, 'Stopped stream has no rest.'
        if not self._evaluated:
            self._rest = self._rest_fn()
            self._evaluated = True
        return self._rest

    def __repr__(self):
        if self.stopped:
            return '<Stream stopped>'
        return 'Stream({0}, <rest_fn>)'.format(repr(self.head))

def map_stream(fn, s):
    if s.stopped:
        return s
    def rest_fn():
        return map_stream(fn, s.rest)
    return Stream(fn(s.head), rest_fn)

def filter_stream(fn, s):
    if s.stopped:
        return s
    def rest_fn():
        return filter_stream(fn, s.rest)
    if fn(s.head):
        return Stream(s.head, rest_fn)
    return rest_fn()

# follower and candidate needs it to gather votes or append_entries.
# randseq stream never stop.
def make_randseq_stream(seed_number, minimum, maximum):
    head = uniform(minimum, maximum)
    rest = lambda: make_randseq_stream(head, minimum, maximum)
    return Stream(head, rest)

# leader needs this to send periodical broadcast.
# fixednum stream never stop.
def make_fixednum_stream(fixed_num):
    return Stream(fixed_num, lambda: fixed_num)

# leader needs this to step down to follower
def make_countdown_stream(start):
    exit = start <= 0
    rest = lambda: make_countdown_stream(start - 1)
    return Stream(start, rest, exit=exit)

# all server need this to send datagram to other servers.
def make_udp_server(config):
    bind = config['bind']
    host, port = bind.split(':')
    host, port = host or '0.0.0.0', int(port)
    udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_server.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    udp_server.bind((host, port))
    return udp_server

# the udp server create a stream based on the timeout stream.
# it gives an rpc_data if receives stuff before timeout.
# it gives null if timeout.

def log_transition(state, from_, to):
    logger.info(f'{state.config["id"]} {state.config["bind"]} convert from {from_} to {to}.')

def make_state_stream(config, state=None, role=None, udp_server=None, state_stream=None):
    start_epoch = time()
    udp_server = udp_server or make_udp_server(config)
    state = state or State(config=config, current_term=0, voted_for=None,
                           commit_index=0, last_applied=0, next_index=[], match_index=[],
                           log=[{'term': 0, 'cmd': {}}])
    role = role or 'follower'
    state_stream = state_stream or make_follower_stream(udp_server, state)

    if role == 'follower' and not state_stream.stopped:
        state_stream = state_stream.rest
    elif role == 'follower' and state_stream.stopped:
        log_transition(state, 'follower', 'candidate')
        role, state_stream = 'candidate', Stream(state, lambda: make_candidate_stream(udp_server, state))

    elif role == 'candidate' and not state_stream.stopped:
        state_stream = state_stream.rest
    elif role == 'candidate' and state_stream.exit == 'new_election':
        role, state_stream = 'candidate', make_candidate_stream(udp_server, state)
    elif role == 'candidate' and state_stream.exit == 'to_leader':
        logger.debug('leader got state: {state}, {state_stream}')
        role, state_stream = 'leader', make_leader_stream(udp_server, state)
        log_transition(state, 'candidate', 'leader')
    elif role == 'candidate' and state_stream.exit == 'to_follower':
        role, state_stream = 'follower', make_follower_stream(udp_server, state)
        log_transition(state, 'candidate', 'follower')
    elif role == 'candidate':
        raise Exception('candidate stream bad condition. head=%s exit=%s', state_stream.head, state_stream.exit)

    elif role == 'leader' and not state_stream.stopped:
        state_stream = state_stream.rest
    elif role == 'leader' and state_stream.stopped:
        role, state_stream = 'follower', make_follower_stream(udp_server, state)
        log_transition(state, 'leader', 'follower')

    state = state_stream.head

    def rest():
        return make_state_stream(config, state, role, udp_server, state_stream)

    return Stream((role, state), rest, exit=None)

def elect_self(udp_server, state):
    state = State.from_(state, current_term=state.current_term+1,
                        voted_for=state.config['id'])
    broadcast(udp_server, state.config['peers'], {'type': 'request_vote', 'term': state.current_term,
        'candidate_id': state.config['id'],
        'last_log_index': len(state.log),
        'last_log_term': state.log[-1]['term'],
    })
    #logger.debug(f'{state.config["id"]} started election at term={state.current_term}.')
    return state

def grant_vote(state, udp_server, rpc_data):
    data = rpc_data.payload
    logger.debug(f'try to grant vote: {data}, {state}')
    if data['term'] > state.current_term:
        sendto(udp_server, rpc_data.address, {
            'type': 'request_vote_response',
            'term': state.current_term,
            'vote_granted': True,
        })
        return True
    elif ((not state.voted_for or state.voted_for == data['candidate_id'])
            and data['last_log_term'] <= state.log[-1]['term']):
        logger.debug(f'{state.config["id"]} voted for {data["candidate_id"]}')
        sendto(udp_server, rpc_data.address, {
            'type': 'request_vote_response',
            'term': state.current_term,
            'vote_granted': True,
        })
        return True
    else:
        sendto(udp_server, rpc_data.address, {
            'type': 'request_vote_response',
            'term': state.current_term,
            'vote_granted': False,
        })
        return False

def follower_respond_rpc(udp_server, response, state):
    data = response.payload
    if data['type'] == 'request_vote':
        if grant_vote(state, udp_server, response):
            state = State.from_(state, voted_for=data['candidate_id'])
        return state
    elif data['type'] == 'append_entries':
        if data['term'] < state.current_term:
            success = False
        elif data['prev_log_index'] > len(state.log):
            success = False
        else:
            # please handle append_entries rpc .3.
            state = State.from_(state, log=log[:data['prev_log_index']] + data['entries'],)
            if data['leader_commit'] > state.commit_index:
                state = State.from_(state,
                        commit_index=min(data['commit_index'], len(state.log)))
        sendto(udp_server, response.address, {
            'type': 'append_entries_response',
            'term': state.current_term,
            'success': True,
        })
        return state
    else:
        raise ValueError('unknown rpc type: %s' % response['type'])

def make_follower_stream(udp_server, state, timer_stream=None, rpc_stream=None):
    timer_stream = timer_stream or make_randseq_stream(time(), 1.50, 4.50)
    rpc_stream = rpc_stream or make_rpc_stream(udp_server, timer_stream)
    if rpc_stream.stopped: # -> candidate (timeout, start election)
        state = elect_self(udp_server, state)
    else: # -> respond append_entries & request_vote
        response = rpc_stream.head
        state = follower_respond_rpc(udp_server, response, state) # @follower
    def rest():
        return make_follower_stream(udp_server, state, timer_stream, rpc_stream)
    return Stream(state, rest, exit=rpc_stream.exit)

def candidate_handle_request_vote_response(state, rpc_data, voted):
    if rpc_data.payload['type'] == 'request_vote_response':
        if rpc_data.payload['vote_granted']:
            voted.add(rpc_data.address)
        if len(voted) > (len(state.config['peers']) + 1) / 2:
            return state, 'to_leader'

def candidate_handle_append_entries(state, rpc_data):
    if rpc_data.payload['type'] == 'append_entries':
        if state.current_term == rpc_data.payload['term']:
            return state, 'to_follower'

def candidate_handle_request_vote(state, udp_server, rpc_data):
    data = rpc_data.payload
    if data['type'] == 'request_vote':
        if grant_vote(state, udp_server, rpc_data):
            state = State.from_(state, voted_for=data['candidate_id'])
            return state, 'to_follower'

def candidate_respond_rpc(state, udp_server):
    voted = set()
    start = time()
    for rpc_data in keep_receiving(udp_server, period=0.300):
        ret = candidate_handle_request_vote_response(state, rpc_data, voted)
        if ret:
            return ret
        ret = candidate_handle_append_entries(state, rpc_data)
        if ret:
            return ret
        ret = candidate_handle_request_vote(state, udp_server, rpc_data)
        if ret:
            return ret

    return state, 'new_election'

def make_candidate_stream(udp_server, state):
    next_state = elect_self(udp_server, state)
    next_state, exit = candidate_respond_rpc(next_state, udp_server)
    def rest():
        return make_candidate_stream(udp_server, next_state)
    return Stream(next_state, rest, exit=exit)

def leader_respond_rpc(udp_server, state, period):
    broadcast(udp_server, state.config['peers'], {
        'type': 'append_entries',
        'term': state.current_term,
        'leader_id': state.config['id'],
        'commit_index': state.commit_index,
    })
    for rpc_data in keep_receiving(udp_server, period=period):
        pass
    return state

def make_leader_stream(udp_server, state, heartbeat_stream=None):
    state = leader_respond_rpc(udp_server, state, 0.300)
    def rest():
        return make_leader_stream(udp_server, state, heartbeat_stream)
    return Stream(state, rest) # Can leader rule life-long? Good question.

@dataclass
class State:

    config: dict

    current_term: int
    voted_for: str
    log: list

    commit_index: int
    last_applied: int

    next_index: list
    match_index: list

    @classmethod
    def from_(cls, state, **kwargs):
        return cls(
            config=kwargs.get('config') or state.config,
            current_term=kwargs.get('current_term') or state.current_term,
            voted_for=kwargs.get('voted_for') or state.voted_for,
            log=kwargs.get('log') or state.log,
            commit_index=kwargs.get('commit_index') or state.commit_index,
            last_applied=kwargs.get('last_applied') or state.last_applied,
            next_index=kwargs.get('next_index') or state.next_index,
            match_index=kwargs.get('match_index') or state.match_index,
        )


# server rule 1: apply those not applied to local log.
def validate_commit_index(state_machine, state):
    last_applied = state.last_applied
    if state.commit_index > state.last_applied:
        for not_applied in range(state.last_applied + 1, state.commit_index + 1):
            command = state.log[not_applied]['command']
            state_machine.update(command)
            last_applied += 1
    return State.from_(state, last_applied=last_applied)

# server rule 2:
# * accept the other leader (by turning itself as follower)
# * reject the other leader (by sending a response with `success=false`)
def validate_term(udp_server, peers, data, state):
    if data['term'] > state.term:
        state = State.from_(state, term=data['term'], role='follower')
    elif data['term'] < state.term and data.get('request'):
        broadcast(udp_server, peers, {
            'type': data['type'],
            'response': True,
            'term': state.term,
            'success': False,
        })
    return state

@dataclass
class RpcData:

    payload: dict
    address: str

def get_rpc_data(udp_server, timeout):
    if timeout <= 0:
        return None
    else:
        #logger.debug(f'try to receive from {udp_server} in {timeout} seconds.')
        udp_server.settimeout(timeout)
        try:
            datagram, address = udp_server.recvfrom(8192)
            #logger.debug(f'{udp_server} received datagram.')
            return RpcData(json.loads(datagram.decode()), address)
        except socket.timeout:
            #logger.debug(f'{udp_server} received nothing.')
            return None

def keep_receiving(udp_server, period):
    start = time()
    count = 0
    left = period
    while left > 0:
        count += 1
        rpc_data = get_rpc_data(udp_server, left)
        if rpc_data is None:
            break
        #logger.debug(f'{udp_server} received {count} datagrams in {period} seconds. This datagram is {rpc_data.payload}')
        yield rpc_data
        left -= (time() - start)

def make_rpc_stream(udp_server, timeout_stream):
    rpc_data = get_rpc_data(udp_server, timeout_stream.head)
    rest = lambda: make_rpc_stream(udp_server, timer_stream.rest)
    return Stream(rpc_data, rest, exit=rpc_data is None)

def sendto(udp_server, target, data):
    encode = json.dumps(data).encode()
    try:
        if isinstance(target, str):
            host, port = target.split(':')
            host, port = host or '0.0.0.0', int(port)
            udp_server.sendto(encode, (host, port))
        else:
            udp_server.sendto(encode, target)
        logger.debug(f'udp server had sent {encode} to {target}.')
    except socket.gaierror:
        logger.debug(f'udp server failed sending {encode} to {target}.')


def broadcast(udp_server, targets, data):
    for target in targets:
        sendto(udp_server, target, data)


def main(bind, peers):
    now = time()
    logger.info('server {bind} started at {now}.')
    state_stream = make_state_stream({
        'id': str(uuid4()),
        'bind': bind,
        'peers': peers,
    })
    while True:
        role, state = state_stream.head
        state_stream = state_stream.rest

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2:])
