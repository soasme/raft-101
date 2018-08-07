import json
import sys
import socket
import logging
import itertools as it
from time import time, sleep
from random import uniform, seed
from functools import partial
from dataclasses import dataclass

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class Stream(object):
    """A lazily computed recursive list."""
    def __init__(self, first, compute_rest, empty=False):
        self.first = first
        self._compute_rest = compute_rest
        self.empty = empty
        self._rest = None
        self._computed = False
    @property
    def rest(self):
        """Return the rest of the stream, computing it if necessary."""
        assert not self.empty, 'Empty streams have no rest.'
        if not self._computed:
            self._rest = self._compute_rest()
            self._computed = True
        return self._rest
    def __repr__(self):
        if self.empty:
            return '<empty stream>'
        return 'Stream({0}, <compute_rest>)'.format(repr(self.first))

def map_stream(fn, s):
    if s.empty:
        return s
    def compute_rest():
        return map_stream(fn, s.rest)
    return Stream(fn(s.first), compute_rest)

def filter_stream(fn, s):
    if s.empty:
        return s
    def compute_rest():
        return filter_stream(fn, s.rest)
    if fn(s.first):
        return Stream(s.first, compute_rest)
    return compute_rest()


def make_randseq_stream(seed_number, minimum, maximum):
    seed(seed_number)
    rand = uniform(minimum, maximum)
    return Stream(rand,
                  partial(make_randseq_stream, rand, minimum, maximum))

@dataclass
class State:

    current_term: int
    voted_for: str
    log: list

    commit_index: int
    last_applied: int

    next_index: list
    match_index: list

    @classmethod
    def from_(state, **kwargs):
        return State(
            current_term=kwargs.get('current_term') or state.current_term,
            voted_for=kwargs.get('voted_for') or state.voted_for,
            log=kwargs.get('log') or state.log,
            commit_index=kwargs.get('commit_index') or commit_index,
            last_applied=kwargs.get('last_applied') or last_applied,
            next_index=kwargs.get('next_index') or next_index,
            match_index=kwargs.get('match_index') or match_index,
        )

def make_udp_server(config):
    bind = config['bind']
    host, port = bind.split(':')
    host, port = host or '0.0.0.0', int(port)
    udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_server.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    udp_server.bind((host, port))
    return udp_server

def validate_commit_index(state_machine, state):
    last_applied = state.last_applied
    if state.commit_index > state.last_applied:
        for not_applied in range(state.last_applied + 1, state.commit_index + 1):
            command = state.log[not_applied]['command']
            state_machine.update(command)
            last_applied += 1
    return State.from_(state, last_applied=last_applied)

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


def make_raft_stream(config):
    start_epoch = time()
    state_machine = {}
    state = State(current_term=0, voted_for=None, log=[], commit_index=0,
                  last_applied=0, next_index=[], match_index=[], role='follower')

    def get_raft_next_state():
        next_state = apply_uncommited_logs(state_machine, state)
        return next_state

    return Stream(state, get_raft_next_state)

def gen_randtime_stream(init_time, minimum, maximum):
    seed(init_time)
    while True:
        yield uniform(minimum, maximum)

def gen_follower_stream(start_epoch, udp_server, bind):
    election_timeout_stream = gen_randtime_stream(start_epoch, 0.150, 0.300)
    for election_timeout in election_timeout_stream:
        udp_server.settimeout(election_timeout)
        try:
            datagram = udp_server.recvfrom(8192)
            # TODO: handle upstream commands
            yield {'datagram': datagram}
        except socket.timeout:
            logger.debug(f'udp socket timeout after {election_timeout} seconds.')
            return

def broadcast(udp_server, targets, data):
    for target in targets:
        encode = json.dumps(data).encode()
        host, port = target.split(':')
        host, port = host or '0.0.0.0', int(port)
        udp_server.sendto(encode, (host, port))
        logger.debug(f'udp server has send {encode} to {target}.')

def keep_receiving(udp_server, timeout):
    start = time()
    while timeout > 0:
        try:
            udp_server.settimeout(timeout)
            datagram, addr = udp_server.recvfrom(8192)
            logger.debug(f'udp socket has received a datagram.')
            yield json.loads(datagram.decode()), addr
            timeout -= (time() - start)
        except socket.timeout:
            logger.debug(f'udp socket timeout after {timeout} seconds when keep receiving datagrams.')
            return

def gen_candidate_stream(start_epoch, udp_server, bind, peers):
    election_timeout_stream = gen_randtime_stream(start_epoch, 0.150, 0.300)
    for election_timeout in election_timeout_stream:
        request_vote = {'type': 'request_vote', 'candidate_id': bind, }
        broadcast(udp_server, peers, request_vote)
        data_stream = keep_receiving(udp_server, election_timeout)
        data = list(data_stream)
        if not data:
            logger.debug(f'udp socket receives nothing so we create another request vote.')
            continue
        else:
            # TODO: handle votes
            yield {'votes': data}

def gen_leader_stream(start_epoch, udp_server, bind, peers):
    heartbeat_stream = gen_randtime_stream(start_epoch, 0.150, 0.300)
    for heartbeat in heartbeat_stream:
        append_entries = {'type': 'append_entries', 'leader_id': bind, }
        broadcast(udp_server, peers, append_entries)
        data_stream = keep_receiving(udp_server, heartbeat_timeout)
        data = list(data_stream)
        if not data:
            logger.debug(f'udp socket receives nothing so we create another append entries.')
            continue
        else:
            # TODO: handle append entries responses.
            yield {'res': data}

            # TODO: accept another leader.
            current_term = 0 # = ?
            for el in data:
                if el['term'] > current_term:
                    return

def gen_raft_stream(start_epoch, udp_server, bind, peers):
    follower_stream = gen_follower_stream(start_epoch, udp_server, bind)
    for follower_state in follower_stream:
        if follower_state is None:
            break
        yield dict(role='follower', bind=bind, **(follower_state or {}))
    candidate_stream = gen_candidate_stream(start_epoch, udp_server, bind, peers)
    for candidate_state in candidate_stream:
        if candidate_state is None:
            break
        yield dict(role='candidate', bind=bind, peers=peers, **(candidate_state or {}))
    leader_stream = gen_leader_stream(start_epoch, udp_server, bind, peers)
    for leader_state in leader_stream:
        if leader_state is None:
            break
        yield dict(role='leader', bind=bind, peers=peers)


def main(bind, peers):
    start_epoch = time()

    host, port = bind.split(':')
    host, port = host or '0.0.0.0', int(port)
    udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_server.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    udp_server.bind((host, port))

    randseqs = make_randseq_stream(start_epoch, 0.150, 0.300)
    print(randseqs.first)
    print(randseqs.rest.first)
    # while True:
        # raft_stream = gen_raft_stream(start_epoch, udp_server, bind, peers)
        # for raft_state in raft_stream:
            # logger.debug(f'cluster current state: {raft_state}')



if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2:])
