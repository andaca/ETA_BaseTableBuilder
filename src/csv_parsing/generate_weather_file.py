from collections import defaultdict
import csv
from datetime import datetime
from processing_tools import read_csv


def bin_by_hour(data):
    for d in data:
        if d['hour'] >= 5 and d['hour'] <= 12:
            d['t_bin'] = 'am'
        elif d['hour'] >= 13 and d['hour'] <= 20:
            d['t_bin'] = 'pgit m'
        else:
            d['t_bin'] = 'night'
        yield d


def batch_it(data):
    batch = [next(data)]
    for d in data:
        if d['t_bin'] != batch[-1]['t_bin'] or d['date'] != batch[-1]['date']:
            yield batch
            batch_ = []
        batch.append(d)


def make_rows(batches):
    for bat in batches:

        # average the wind, rain, temp and cloud values in the abtch
        totals = defaultdict(float)
        for key in ['rain', 'wind', 'temp', 'cloud']:
            for data in bat:
                totals[key] += data[key]

        for key, val in totals.items():
            totals[key] = round(val / len(bat), 2)

        # yield a single row (list)
        yield [bat[0]['date'], bat[0]['t_bin'], totals['cloud'],
               totals['rain'], totals['temp'], totals['wind']]


def main():
    rows = read_csv('../data/hly532/hly532.csv')
    # select relevant months

    rows = filter(
        lambda row: '/11/2012' in row[0] or '/01/2013' in row[0], rows)

    data = ({
        'date': row[0].split()[0],
        'hour': int(row[0].split(':')[0][-2:]),
        'rain': float(row[2]),
        'temp': float(row[4]),
        'wind': float(row[12]),
        'cloud': int(row[20])
    } for row in rows)

    data = bin_by_hour(data)
    batches = batch_it(data)

    rows = make_rows(batches)

    print('writing data...')
    with open('../data/weather_output.csv', 'wt') as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)
    print('Done.')


if __name__ == '__main__':
    main()
