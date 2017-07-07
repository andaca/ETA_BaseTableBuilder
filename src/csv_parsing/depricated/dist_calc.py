"""Adds the distance travelled from the start of the journey for every rown in 
the input file. Writes to a new csv file. The row is unchanged, except for the
distance_travelled value appended to the end of each row."""

import csv

from geopy.distance import distance


def read_csv(fname):
    with open(fname, 'rt', newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            yield row


def csv_writer_coroutine(fname, close_sig='CLOSE'):
    """Coroutine. Opens specified file for writing as csv. waits to receive rows 
    (lists) by the send() method. File is closed when 'CLOSE' is sent.
    @raises :  StopIteration
    """
    with open(fname, 'wt') as f:
        writer = csv.writer(f)
        while True:
            row = yield
            if row == close_sig:
                f.close()
                break
            writer.writerow(row)


def handler(handler_name, writer):
    """Coroutine. accepts rows via send() method, and does something with it.
    If stop_value is passed, raises StopIteration when that vlue is recieved.
    args : handler_name (str), writer (a csv_writer)
    """

    # this is all handler_name is used for. If you don't want to print,
    # you can remove the argument completely.
    print('Coroutine started: {}'.format(handler_name))

    # position in row of coordinates
    lat, lon = 9, 8

    # read the first value
    prev = yield
    running_dist = 0
    writer.send([*prev, running_dist])

    while True:
        row = yield  # receive a row via the send() method

        dist = distance((row[lat], row[lon]), (prev[lat], prev[lon])).meters
        running_dist = round(running_dist + dist, 2)

        writer.send([*row, running_dist])
        print([*row, running_dist])

        prev = row


def main():
    fname = '../data/siri.20121106.csv'

    writer = init_coroutine(csv_writer, '../data/output.csv')

    # stores a handler for a journeyPatternId::vehicleJourneyId
    coroutines = {}

    rows = read_csv(fname)
    for row in rows:
        jpId = row[3]
        vjId = row[5]

        bad_values = ['null', None, '', ' ']
        if jpId in bad_values or vjId in bad_values:
            continue

        key = '::'.join([jpId, vjId])

        try:
            coroutines[key].send(row)
        except KeyError:
            coroutines[key] = init_coroutine(handler, key, writer)
            coroutines[key].send(row)

    try:
        writer.send('CLOSE')
    except StopIteration:
        print('Done!')


if __name__ == '__main__':
    main()
