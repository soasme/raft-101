from time import time

from rx import Observable, Observer


def main():
    start_epoch = time()
    Observable.just(start_epoch).subscribe(print)

if __name__ == '__main__':
    main()
