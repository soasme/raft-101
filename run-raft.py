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
logging.basicConfig(level=logging.DEBUG)

class Stream(object):
    def __init__(self, head, rest_fn, exit=None):
        self.head = head
        self.exit = exit
        self._rest = None
        self._rest_fn = rest_fn
        self._evaluated = False

    @property
    def stopped(self):
        return self.exit is None

    @property
    def rest(self):
        assert self.stopped, 'Stopped streams have no rest.'
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
        role, state_stream = 'candidate', make_candidate_stream(udp_server, state)
        logger.info('candidate stream stopped? %s', state_stream.stopped)

    elif role == 'candidate' and state_stream.exit == 'new_election':
        role, state_stream = 'candidate', make_candidate_stream(udp_server, state)
    elif role == 'candidate' and state_stream.exit == 'to_leader':
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
    return Stream((role, state), rest)

def elect_self(udp_server, state):
    logger.debug(f'start election from term={state.current_term}')
    state = State.from_(state, current_term=state.current_term+1,
                        voted_for=state.config['id'])
    broadcast(udp_server, state.config['peers'], {'type': 'request_vote', 'term': state.current_term,
        'candidate_id': state.config['id'],
        'last_log_index': len(state.log),
        'last_log_term': state.log[-1]['term'],
    })
    logger.debug(f'{state.config["id"]} started election at term={state.current_term}.')
    return state

def make_follower_stream(udp_server, state, timer_stream=None, rpc_stream=None):
    timer_stream = timer_stream or make_randseq_stream(time(), 0.150, 0.300)
    rpc_stream = rpc_stream or make_rpc_stream(udp_server, timer_stream)
    logger.debug(rpc_stream.stopped and 'candidate rpc stream stopped.' or 'xxx')
    if rpc_stream.stopped: # -> candidate (timeout, start election)
        state = elect_self(udp_server, state)
    else: # -> respond append_entries & request_vote
        response = rpc_stream.head
        state = follower_respond_rpc(response, state) # @follower
    def rest():
        return make_follower_stream(udp_server, state, timer_stream, rpc_stream)
    return Stream(state, rest, exit=rpc_stream.exit)

def make_candidate_stream(udp_server, state, timer_stream=None, rpc_stream=None):
    timer_stream = make_fixednum_stream(0.300)
    rpc_stream = rpc_stream or make_rpc_stream(udp_server, timer_stream)
    if rpc_stream.stopped: # -> restart election
        next_state, exit = elect_self(udp_server, state), 'new_election'
    else:
        next_state, exit = candidate_respond_rpc(rpc_stream, state) # ->leader, ->follower
    def rest():
        return make_candidate_stream(udp_server, next_state)
    return Stream(next_state, rest, exit=exit)

def make_leader_stream(udp_server, state, heartbeat_stream=None,
        timer_stream=None, rpc_stream=None):
    heartbeat_stream = heartbeat_stream or make_fixednum_stream(0.300)
    timer_stream = make_fixednum_stream(0.300)
    rpc_stream = rpc_stream or make_rpc_stream(udp_server, timer_stream)
    state = leader_respond_rpc(rpc_stream, state)
    def rest():
        return make_leader_stream(udp_server, state, heartbeat_stream, timer_stream, rpc_stream)
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
        udp_server.settimeout(timeout)
        try:
            datagram, address = udp_server.recvfrom(8192)
            return RpcData(json.loads(datagram.decode()), address)
        except socket.timeout:
            return None

def make_rpc_stream(udp_server, timeout_stream):
    rpc_data = get_rpc_data(udp_server, timeout_stream.head)
    rest = lambda: make_rpc_stream(udp_server, timer_stream.rest)
    return Stream(rpc_data, rest)

def make_consequent_rpc_stream(udp_server, period):
    address = None
    if timeout <= 0:
        rpc_data = None
    else:
        start_epoch = time()
        udp_server.settimeout(period)
        try:
            datagram, address = udp_server.recvfrom(8192)
            period -= (time() - start_epoch)
            rpc_data = RpcData(json.loads(datagram.decode()), address)
        except socket.timeout:
            rpc_data = None
    return Stream(rpc_data,
                  partial(make_consequent_rpc_stream, udp_server, period))

def make_periodical_rpc_stream(udp_server, timeout):
    udp_server.settimeout(timeout)
    try:
        datagram, address = udp_server.recvfrom(8192)
        rpc_data = RpcData(json.loads(datagram.decode()), address)
    except socket.timeout:
        rpc_data = None
    return Stream(rpc_data, partial(make_periodical_rpc_stream, udp_server, timeout))


def apply_server_rules(config, state, state_machine, udp_server, rpc_stream):
    state = validate_commit_index(state_machine, state)
    rpc_data = rpc_stream.head
    state = validate_term(udp_server, config['peers'], rpc_data, state)
    if state.role == 'follower':
        state = follower_respond_rpc(udp_server, state)
        if rpc_data is None:
            state = State.from_(state, role='candidate')
            state = start_election(udp_server, state)
    elif state.role == 'candidate':
        votes = collect_votes(rpc_stream)
        if is_majority_accepted(votes):
            state = State.from_(state, role='leader')
            send_initial_heartbeat(udp_server, state)
        elif is_append_entries_received(votes):
            state = State.from_(state, role='follower')
        elif is_no_votes_received(votes):
            state = start_election(udp_server, state)
    elif state.role == 'leader':
        pass
    return state

def broadcast(udp_server, targets, data):
    for target in targets:
        encode = json.dumps(data).encode()
        host, port = target.split(':')
        host, port = host or '0.0.0.0', int(port)
        udp_server.sendto(encode, (host, port))
        logger.debug(f'udp server has send {encode} to {target}.')


def main(bind, peers):
    state_stream = make_state_stream({
        'id': str(uuid4()),
        'bind': bind,
        'peers': peers,
    })
    while True:
        state = state_stream.head
        state_stream = state_stream.rest


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2:])
