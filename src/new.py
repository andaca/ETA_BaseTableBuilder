import json
from collections import defaultdict
import csv

from geopy.distance import distance


min_dist = 0.1  # km


def read_csv(fname):
    """Open csv file. Yields lists. file closed automatically."""
    with open(fname, 'rt', newline='') as f:
        reader = csv.reader(f)
        for r in reader:
            yield r


def main():
    b_jour_patt_id = 3
    b_lon = 8
    b_lat = 9

    bus_table = read_csv('../data/siri.20121106.csv')
    stop_table = read_csv('../data/stopinfo.csv')
    rd_file = open('../data/routes.txt')
    route_data = json.load(rd_file)

    # dict keys of the stopId, value is [lat, lon] list
    stop_dict = defaultdict(list)
    for r in stop_table:
        stop_dict[str(r[4])] = [r[7], r[8]]

    # output file
    of = open('output.csv', 'wt')
    writer = csv.writer(of)

    count = 0
    for row in bus_table:
        count += 1
        print('\r{} rows compared.'.format(count), end='')
        vpId = row[3]

        try:
            stops = route_data[vpId]['stops']
        except KeyError:
            continue

        for stop in stops:
            stop = str(stop)

            dist = distance((row[b_lat], row[b_lon]), stop_dict[stop]).meters
            dist = round(dist / 1000, 2)
            if dist < min_dist:
                row.extend([stop, dist])
                writer.writerow(row)

    of.close()
    rd_file.close()


if __name__ == '__main__':
    main()
