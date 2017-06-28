from collections import namedtuple
import csv

infile = '../data/all_stop_info.csv'
outfile = '../data/2017_stops.csv'


def bus_stops(filename):
    with open(filename, 'rt', newline='') as f:
        reader = csv.reader(f)
        for r in reader:
            yield (r[9], r[2], r[4], r[5])


def main():
    stops = (bus_stops(infile))
    next(stops)
    of = open(outfile, 'wt')
    writer = csv.writer(of)
    for bs in stops:
        try:
            int(bs[0])
            writer.writerow(bs)
        except ValueError:
            break


if __name__ == '__main__':
    main()
