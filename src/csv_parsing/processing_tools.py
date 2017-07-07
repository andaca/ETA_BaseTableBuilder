from collections import defaultdict
import csv

import gzip

"""
def read_csv(filename, named_tuple=None):
    \"""Generator function. Reads a CSV file(optionally gzipped) and yields it's
    contents. If a named_tuple is passed, the rows are returned as those. Else
    returned as lists.

    @params: filename(str), named_tuple(namedtuple, declared in the 'collections' module)
    @yields: lists or namedtuple
    @raises: ValueError
    \"""
    if filename.endswith('gz'):
        f = gzip.open(filename, 'rt')
    elif filename.endswith('csv'):
        f = open(filename, 'rt', newline='')
    else:
        raise ValueError('{} is not a csv or gz file'.format(filename))
    reader = csv.reader(f)
    for row in reader:
        if named_tuple is not None:
            yield named_tuple(*row)
        else:
            yield row
    f.close()
"""


def read_csv(fname):
    with open(fname, 'rt', newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            yield row


def add_weather(rows, error_writer=None, weather_file='../../data/weather_output.csv', ):
    """TODO: Can I move the file reading portion out of the function,
    so it only gets called at import time?"""

    # read weather data from csv file
    weather_dict = defaultdict(list)
    for row in list(read_csv(weather_file)):
        key = ':'.join([row[0], row[1]])  # date, and time_bin
        print(key)
        input()
        weather_dict[key].extend(row[2:])

    print(weather_dict)
    input()
    for row in rows:
        date_str = row['dt'].strftime('%d/%m/%Y')

        try:
            if row['dt'].hour >= 5 and row['dt'].hour <= 12:
                weather = weather_dict[date_str]['am']

            elif row['dt'].hour >= 13 and row['dt'].hour <= 20:
                weather = weather_dict[date_str]['pm']

            else:
                weather = weather_dict[date_str]['night']

        except KeyError as e:
            if error_writer:
                err_str = '{} :: {}'.format(e, row)
                error_writer.send(row)
            else:
                print('{} :: {}'.format(e, row))

            print(date_str)
            print(row['dt'].hour)
            try:
                print(weather_dict[date_str])
            except KeyError:
                print('date not in dict')
            input()
            yield None
            continue

        row['cloud'] = weather[0]
        row['rain'] = weather[1]
        row['temp'] = weather[2]
        row['wind'] = weather[3]

        yield row
