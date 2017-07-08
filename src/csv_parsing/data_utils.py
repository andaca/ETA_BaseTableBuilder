import csv
from functools import wraps


def grow_dict(dictionary, keys, values):
    for k, v in zip(keys, values):
        dictionary[k] = v
    return dictionary


def read_csv(fname):
    with open(fname, 'rt', newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            yield row


def coroutine(function):
    """A nice coroutine wrapper, 
    described by L. Ramalho in `Fluent Python`, p. 474"""

    @wraps(function)
    def primer(*args, **kwargs):
        """prime the coroutine"""
        func = function(*args, **kwargs)
        next(func)
        return func
    return primer


@coroutine
def coro_csv_writer(fname, count=True):
    """Opens specified file for writing as csv. waits to receive rows
    (lists) by the send() method.
    File is closed when StopIteration is sent
    """
    count_ = 0
    with open(fname, 'wt') as f:
        try:
            writer = csv.writer(f)
            while True:
                row = yield
                if row is StopIteration:
                    break
                writer.writerow(row)
                count_ += 1
                if count:
                    print('\r {:,} rows written'.format(count_), end='')
        finally:
            f.close()


@coroutine
def coro_print():
    while True:
        rcvd = yield
        print(rcvd)
