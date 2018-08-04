from time import time, sleep
from random import random, seed

from rx import Observable, Observer

def gen_random_seq(seed_number):
    seed(seed_number)
    while True:
        val = random()
        sleep(val)
        yield val

def main():
    start_epoch = time()
    seq_stream = gen_random_seq(start_epoch)
    Observable.from_(seq_stream).subscribe(print)

if __name__ == '__main__':
    main()
