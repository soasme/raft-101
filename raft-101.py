import sys
import socket
import logging
from functools import wraps
from uuid import uuid4
from time import sleep, time
from random import uniform
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
    rest = lambda: countdown(period, start)
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
    rest = lambda: keep_receiving(udp_server, second, left)
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
        return cls(
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
                ctx=dict(ctx, server_state='follower'),
            )
        return state
    return _

def validate_commit_index(f):
    @wraps(f)
    def _(state, datagram):
        if state.commit_index > state.last_applied:
            state = state.to(
                last_applied=state.commit_index,
                ctx=dict(ctx, state_machine=apply_state_machine(state))
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
    ctx['id'] = str(uuid4())
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
        next_index=[],
        match_index=[],
        log=[{'term': 0, 'cmd': {}}]
    )

def make_follower_stream(state):
    datagram_stream = keep_receiving(state.ctx['udp_server'], uniform(1.5, 3.0))
    election_timeout = datagram_stream.stopped
    state = reduce_stream(follower_handle_datagram, datagram_stream, state)
    rest = lambda: make_candidate_stream(state) if election_timeout \
            else make_follower_stream(state)
    return Stream(state, rest)

def make_candidate_stream(state):
    return fixednum(3)

def make_leader_stream(state):
    return fixednum(3)

def append_entries(state):
    udp_server = state.ctx['udp_server']
    for peer in state.ctx['peers']:
        prev_log_index = (state.next_index.get(sender) or 0)
        prev_log_term = state.log[prev_log_index]['term']
        entries = state.log[prev_log_index:]
        sendto(udp_server, rpc_data.address, {
            'type': 'append_entries',
            'term': state.current_term,
            'leader_id': state.config['id'],
            'prev_log_index': prev_log_index,
            'prev_log_term': prev_log_term,
            'leader_commit': state.commit_index,
            'entries': entries,
        })

def request_vote(state):
    broadcast(state.ctx['udp_server'], state.ctx['peers'], {
        'type': 'request_vote',
        'term': state.current_term,
        'candidate_id': state.ctx['id'],
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

@validate_term
@validate_commit_index
def follower_handle_datagram(state, datagram):
    if datagram.payload['type'] == 'request_vote':
        return follower_on_append_entries(state, datagram)
    elif datagram.payload['type'] == 'append_entries':
        return follower_on_request_vote(state, datagram)
    else:
        return state


def follower_on_append_entries(state, datagram):
    data = datagram.payload
    if data['term'] < state.current_term \
            or data['prev_log_index'] > len(state.log) - 1:
        reply_append_entries(state, datagram, success=False)
        return state

    if state.log[data['prev_log_index']]['term'] != data['term']:
        state = state.to(log=state.log[:data['prev_log_index']])

    state = state.to(log=state.log + data['entries'])

    if data['leader_commit'] > state.commit_index:
        state = state.to(commit_index=min(data['leader_commit'], len(state.log) - 1))
    reply_append_entries(state, datagram, success=True)
    return state

def follower_on_request_vote(state, datagram):
    data = datagram.payload
    if data['term'] < state.current_term:
        reply_request_vote(state, datagram, vote_granted=False)
        return state
    if state.voted_for in (None, data['candidate_id']) \
            and data['last_log_index'] >= len(state.log) - 1:
        reply_request_vote(state, datagram, vote_granted=True)
    else:
        reply_request_vote(state, datagram, vote_granted=False)
    return state


if __name__ == '__main__':
    bind, peers = sys.argv[1], sys.argv[2:]
    state = init_raft_state({'bind': bind, 'peers': peers})
    logger.info(f'server started at {state.ctx["bind"]}')
    stream = make_raft_server(state)
    while not stream.stopped:
        print(stream.head)
        stream = stream.rest
