import json
import sys
import socket
import logging
from functools import wraps
from time import sleep, time
from random import uniform
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
        return bool(self.exit)

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

def reduce_stream(fn, s, init):
    elem = init
    while not s.stopped:
        elem = fn(elem, s.head)
        s = s.rest
    return elem

def randseq(minimum, maximum):
    rand = uniform(minimum, maximum)
    rest = lambda: randseq(minimum, maximum)
    return Stream(rand, rest)

def fixednum(num):
    return Stream(num, lambda: fixednum(num))

def stopwatch(period, start=None):
    start = start or time()
    head = max(0, period - (time() - start))
    rest = lambda: stopwatch(period, start)
    return Stream(head, rest, exit=not head)

def make_udp_server(bind):
    host, port = bind.split(':')
    host, port = host or '0.0.0.0', int(port)
    udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_server.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    udp_server.bind((host, port))
    return udp_server

def sendto(udp_server, target, data):
    encode = json.dumps(data).encode()
    try:
        if isinstance(target, str):
            host, port = target.split(':')
            host, port = host or '0.0.0.0', int(port)
        else:
            host, port = target
        udp_server.sendto(encode, (host, port))
        logger.debug(f'udp server had sent {encode} to {target}.')
    except socket.gaierror:
        logger.debug(f'udp server failed sending {encode} to {target}.')

def broadcast(udp_server, targets, data):
    for target in targets:
        sendto(udp_server, target, data)

@dataclass
class Datagram:
    address: tuple
    payload: dict

def receive(udp_server, timeout):
    udp_server.settimeout(timeout)
    try:
        datagram, address = udp_server.recvfrom(8192)
        return Datagram(address, json.loads(datagram.decode()))
    except socket.timeout:
        return None

def keep_receiving(udp_server, second, left=None):
    left = left or stopwatch(second)
    data = receive(udp_server, left.head) if not left.stopped else None
    rest = lambda: keep_receiving(udp_server, second, left.rest)
    return Stream(data, rest, exit=data is None)

@dataclass
class State:

    ctx: dict

    current_term: int
    voted_for: str
    log: list

    commit_index: int
    last_applied: int

    next_index: list
    match_index: list

    def to(self, **kwargs):
        return State(
            ctx=kwargs.get('ctx') or self.ctx,
            current_term=kwargs.get('current_term') or self.current_term,
            voted_for=kwargs.get('voted_for') or self.voted_for,
            log=kwargs.get('log') or self.log,
            commit_index=kwargs.get('commit_index') or self.commit_index,
            last_applied=kwargs.get('last_applied') or self.last_applied,
            next_index=kwargs.get('next_index') or self.next_index,
            match_index=kwargs.get('match_index') or self.match_index,
        )

def validate_term(f):
    @wraps(f)
    def _(state, datagram):
        if datagram.payload['term'] > state.current_term:
            state = state.to(
                current_term=datagram.payload['term'],
                ctx=dict(state.ctx, server_state='follower'),
            )
        return state
    return _

def validate_commit_index(f):
    @wraps(f)
    def _(state, datagram):
        if state.commit_index > state.last_applied:
            state = state.to(
                last_applied=state.commit_index,
                ctx=dict(state.ctx, state_machine=apply_state_machine(state))
            )
        return state
    return _

# TODO
def apply_state_machine(state): return {}

def make_raft_server(state=None, state_stream=None):
    stream = state_stream or make_follower_stream(state)
    rest = lambda: make_raft_server(state, stream.rest)
    return Stream(stream.head, rest)

def init_raft_state(ctx):
    ctx['start_epoch'] = time()
    ctx['udp_server'] = make_udp_server(ctx['bind'])
    ctx['server_state'] = 'follower'
    ctx['state_machine'] = {}
    return State(
        ctx=ctx,
        current_term=0,
        voted_for=None,
        commit_index=0,
        last_applied=0,
        next_index={},
        match_index={},
        log=[{'term': 0, 'cmd': {}}]
    )

def make_follower_stream(state):
    datagram_stream = keep_receiving(state.ctx['udp_server'], uniform(1.5, 4.5))
    election_timeout = datagram_stream.stopped
    state = reduce_stream(follower_handle_datagram, datagram_stream, state)
    rest = lambda: make_candidate_stream(state) if election_timeout \
            else make_follower_stream(state)
    return Stream(state, rest)

def make_candidate_stream(state):
    state = elect_self(state)
    datagram_stream = keep_receiving(state.ctx['udp_server'], uniform(1.5, 4.5))
    election_timeout = datagram_stream.stopped
    state = reduce_stream(candidate_handle_datagram, datagram_stream, state)
    def rest():
        if election_timeout:
            return make_candidate_stream(state)
        elif state.ctx['server_state'] == 'follower':
            return make_follower_stream(initialize_follower(state))
        elif state.ctx['server_state'] == 'leader':
            return make_leader_stream(initialize_leader(state))
    return Stream(state, rest)

def make_leader_stream(state):
    state = send_heartbeat(state)
    datagram_stream = keep_receiving(state.ctx['udp_server'], 0.3)
    state = reduce_stream(leader_handle_datagram, datagram_stream, state)
    rest = lambda: make_follower_stream(initialize_follower(state)) \
            if state.ctx['server_state'] == 'follower' else make_leader_stream(state)
    return Stream(state, rest)

def append_entries(state):
    udp_server = state.ctx['udp_server']
    for peer in state.ctx['peers']:
        prev_log_index = (state.next_index.get(peer) or 0)
        prev_log_term = state.log[prev_log_index]['term']
        entries = state.log[prev_log_index:]
        broadcast(udp_server, state.ctx['peers'], {
            'type': 'append_entries',
            'term': state.current_term,
            'leader_id': state.ctx['bind'],
            'prev_log_index': prev_log_index,
            'prev_log_term': prev_log_term,
            'leader_commit': state.commit_index,
            'entries': entries,
        })

def request_vote(state):
    broadcast(state.ctx['udp_server'], state.ctx['peers'], {
        'type': 'request_vote',
        'term': state.current_term,
        'candidate_id': state.ctx['bind'],
        'last_log_index': len(state.log) - 1,
        'last_log_term': state.log[-1]['term'],
    })

def reply_append_entries(state, datagram, success):
    sendto(state.ctx['udp_server'], datagram.address, {
        'type': 'append_entries_response',
        'term': state.current_term,
        'success': bool(success)
    })

def reply_request_vote(state, datagram, vote_granted):
    sendto(state.ctx['udp_server'], datagram.address, {
        'type': 'request_vote_response',
        'term': state.current_term,
        'vote_granted': bool(vote_granted)
    })

def on_receiving_request_vote(state, datagram):
    data = datagram.payload
    if data['term'] < state.current_term:
        reply_request_vote(state, datagram, vote_granted=False)
        return state
    if state.voted_for in (None, data['candidate_id']) \
            and data['last_log_index'] >= len(state.log) - 1:
        reply_request_vote(state, datagram, vote_granted=True)
        state = state.to(ctx=dict(state.ctx, server_state='follower'))
    else:
        reply_request_vote(state, datagram, vote_granted=False)
    return state

def on_receiving_append_entries(state, datagram):
    data = datagram.payload
    if data['term'] < state.current_term \
            or data['prev_log_index'] > len(state.log) - 1:
        reply_append_entries(state, datagram, success=False)
        return state

    if state.log[data['prev_log_index']]['term'] != data['term']:
        state = state.to(log=state.log[:data['prev_log_index']])

    state = state.to(
        log=state.log + data['entries'],
        ctx=dict(state.ctx, server_state='follower'),
    )

    if data['leader_commit'] > state.commit_index:
        state = state.to(commit_index=min(data['leader_commit'], len(state.log) - 1))
    reply_append_entries(state, datagram, success=True)
    return state

def elect_self(state):
    state = state.to(
        current_term=state.current_term+1,
        voted_for=state.ctx['bind'],
        ctx=dict(state.ctx, votes=0, server_state='candidate'),
    )
    request_vote(state)
    return state

@validate_term
@validate_commit_index
def follower_handle_datagram(state, datagram):
    if datagram.payload['type'] == 'request_vote':
        state = on_receiving_request_vote(state, datagram)
        return state
    elif datagram.payload['type'] == 'append_entries':
        return on_receiving_append_entries(state, datagram)
    else:
        return state

@validate_term
@validate_commit_index
def candidate_handle_datagram(state, datagram):
    if state.ctx['server_state'] != 'candidate':
        return state
    elif datagram.payload['type'] == 'request_vote':
        return on_receiving_request_vote(state, datagram)
    elif datagram.payload['type'] == 'append_entries':
        return on_receiving_append_entries(state, datagram)
    elif datagram.payload['type'] == 'request_vote_response':
        return candidate_handle_request_vote_response(state, datagram)
    else:
        return state

@validate_term
@validate_commit_index
def leader_handle_datagram(state, datagram):
    if state.ctx['server_state'] != 'leader':
        return state
    elif datagram.payload['type'] == 'append_entries':
        return on_receiving_append_entries(state, datagram)
    elif datagram.payload['type'] == 'request_vote':
        return on_receiving_request_vote(state, datagram)
    elif datagram.payload['type'] == 'append_entries_response':
        return leader_handle_append_entries_response(state, datagram)
    else: # TODO: apply command for clients.
        return state

def candidate_handle_request_vote_response(state, datagram):
    data = datagram.payload
    if data['term'] == state.current_term:
        state = state.to(ctx=dict(state.ctx, votes=state.ctx['votes'] + 1))
    if state.ctx['votes'] > (state.ctx['peers'] + 1 ) / 2:
        state = state.to(ctx=dict(state.ctx, server_state='leader'))
    return state

def initialize_follower(state):
    return state.to(
        voted_for=None,
    )

def initialize_leader(state):
    return state.to(
        ctx=dict(state.ctx, server_state='leader'),
        next_index={peer: len(state.log) for peer in peers},
        match_index={peer: 0 for peer in peers},
    )

def send_heartbeat(state):
    append_entries(state)
    return state

def leader_handle_append_entries_response(state, datagram):
    data = datagram.payload
    if not data['success']:
        next_index = dict(state.next_index)
        host, port = datagram.address
        hostname = socket.gethostname(host) # TODO
        sender = f'{hostname}:{port}'
        next_index.setdefault(sender, 1)
        next_index[sender] = max(0, next_index[sender]-1)
        state = state.to(next_index=next_index)
    for n in range(state.commit_index+1, len(state.log)):
        cnt = len(id for id, idx in state.match_index.items() if idx >= n)
        if cnt > len(state.peers) + 1 and \
                state.log[n]['term'] == state.current_term:
            state = state.to(commit_index=n)
    return state

if __name__ == '__main__':
    bind, peers = sys.argv[1], sys.argv[2:]
    state = init_raft_state({'bind': bind, 'peers': peers})
    logger.info(f'server started at {state.ctx["bind"]}')
    stream = make_raft_server(state)
    while not stream.stopped:
        state = stream.head
        logger.info('%s %s', time(), state.ctx['server_state'])
        stream = stream.rest
