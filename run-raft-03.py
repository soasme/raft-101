import sys
import socket
import logging
from time import time, sleep
from random import uniform, seed
from functools import partial

from rx import Observable, Observer

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

def gen_timer_stream(seed_number, minimum, maximum):
    seed(seed_number)
    while True:
        yield uniform(minimum, maximum)

def gen_datagram_stream(udp_server, timer_stream):
    def handle_timer(timeout):
        udp_server.settimeout(timeout)
        try:
            return udp_server.recvfrom(4096)
        except socket.timeout:
            logger.debug(f'udp socket timeout after {timeout} seconds.')
    return timer_stream.subscribe(handle_timer)

def main(bind):
    start_epoch = time()

    host, port = bind.split(':')
    host, port = host or '0.0.0.0', int(port)
    udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_server.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    udp_server.bind((host, port))

    timer_stream = Observable.from_(gen_timer_stream(start_epoch, 0.300, 0.500))
    datagram_stream = Observable.from_(gen_datagram_stream(udp_server, timer_stream))
    datagram_stream.subscribe(print)

if __name__ == '__main__':
    main(sys.argv[1])
