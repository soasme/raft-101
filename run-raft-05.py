import json
import sys
import socket
import logging
import itertools as it
from time import time, sleep
from random import uniform, seed
from functools import partial

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

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
            datagram = udp_server.recvfrom(8192)
            logger.debug(f'udp socket has received a datagram.')
            yield json.loads(datagram.decode())
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
            yield {'votes': data}

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


def main(bind, peers):
    start_epoch = time()

    host, port = bind.split(':')
    host, port = host or '0.0.0.0', int(port)
    udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_server.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    udp_server.bind((host, port))

    raft_stream = gen_raft_stream(start_epoch, udp_server, bind, peers)
    for raft_state in raft_stream:
        logger.debug(f'cluster current state: {raft_state}')



if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2:])
