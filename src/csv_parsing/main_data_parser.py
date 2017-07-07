from collections import defaultdict
from datetime import datetime

from geopy.distance import distance

from processing_tools import read_csv, bin_time, WEATHER_DATA, init_coroutine
from processing_tools import csv_writer_coroutine, STOP_COORDS
from processing_tools import JOURNEY_PATTERN_ID_STOPS as JPID_STOPS


def add_weather(data, weather_data):
    """Takes a dict representing a row, looks up the relevant weather information
    for the datetime object contained in 'dt' and adds the relevant key:value pairs.

    @args: data (dict), weather_data (dict)
    @returns: dict
    """

    t_bin = bin_time(data['dt'])
    date = data['dt'].strftime('%d/%m/%Y')
    weather = weather_data[date][t_bin]

    for k, idx in {'cloud': 0, 'rain': 1, 'temp': 2, 'wind': 3}.items():
        try:
            data[k] = weather[idx]
        # TODO: Find out what's wrong! This should probably be addressed in the
        # __get_weather() function in the processing_tools.py file
        except IndexError:
            data[k] = None

    return data


def add_nearby_stop(data, jpId_stops, stop_coords, max_dist=100):
    """takes a dictionary, finds the closest stop for the specified journey
        pattern ID and adds the 'closest_stop' and 'closest_stop_dist' values.
    @args:
        data (dict),
        jpId_stops (dict) *import from processing_tools*,
        stop_coords (dict) *import from processing_tools*,
        max_dist (int) *optional. In meters*
    @returns: dict
    """
    closest_stop = {'stopId': None, 'distance': float('inf')}

    stop_ids = jpId_stops[data['jpId']]
    for stop in stop_ids:
        dist = distance(stop_coords[stop],
                        (data['lat'], data['lon'])).meters
        if dist <= max_dist and dist < closest_stop['distance']:
            closest_stop = {'stopId': stop, 'distance': dist}

    if closest_stop['stopId']:
        data['closest_stop'] = closest_stop['stopId']
        data['closest_stop_dist'] = round(closest_stop['distance'], 2)
    else:
        data['closest_stop'] = None
        data['closest_stop_dist'] = None
    return data


def distance_calculator():
    """Recivies a (lat, lon) value via the send() funtion, and returns the total
    distance traversed since the first value in meters"""

    prev = yield
    dist = 0
    while True:
        this = yield round(dist, 2)
        dist += distance(prev, this).meters
        prev = this


def rowify(data):
    """Takes a data dictionary, and returns a list of the data contained within
    it as it should be written to a csv file."""

    columns = ['dt', 'line', 'jpId', 'timeframe', 'vjId', 'lat', 'lon', 'delay',
               'blockId', 'stopId', 'atStop', 'running_dist', 'running_time',
               'closest_stop', 'closest_stop_dist', 'cloud', 'rain', 'temp', 'wind']

    return [data[c] for c in columns]


def handler(writer):
    """Recieves dictionaries via the send() method. For each dictionary, calculates
    the time passed since the time in the first dictionary, and uses the distance_calculator
    to calculate the distance travelled. Adds these values to the dictionary, and
    then sends the dictionary to the writer coroutine.

    @args: writer (coroutine)
    """

    dist_calc = init_coroutine(distance_calculator)
    start_time = None

    while True:
        data = yield

        if start_time is None:
            start_time = data['dt']

        data['running_dist'] = dist_calc.send((data['lat'], data['lon']))
        data['running_time'] = (data['dt'] - start_time).total_seconds()

        if data['closest_stop'] is not None:
            writer.send(rowify(data))


# for debugging purposes
def print_coroutine():
    while True:
        rcvd = yield
        print(rcvd)
        input()


def process(rows):
    datas = ({
        'dt': datetime.fromtimestamp(int(row[0][:-6])),
        'line': row[1],
        'jpId': row[3],
        'timeframe': row[4],
        'vjId': row[5],
        'lat': row[9],
        'lon': row[8],
        'delay': row[10],
        'blockId': row[11],
        'stopId': row[13],
        'atStop': row[14]
    } for row in rows)

    datas = (add_weather(data, WEATHER_DATA)
             for data in datas)

    datas = (add_nearby_stop(data, JPID_STOPS, STOP_COORDS)
             for data in datas)

    # use a coroutine for a csv writer to ensure the file is only opened once,
    # and written to by only one function
    writer = init_coroutine(csv_writer_coroutine,
                            '../../data/reduced_data.csv')

    # writer = init_coroutine(print_coroutine)  #write to screen instead of file

    columns = ['dt', 'line', 'jpId', 'timeframe', 'vjId', 'lat', 'lon', 'delay',
               'blockId', 'stopId', 'atStop', 'running_dist', 'running_time',
               'closest_stop', 'closest_stop_dist', 'cloud', 'rain', 'temp', 'wind']
    writer.send(columns)  # write column headers

    handlers = {}

    for data in datas:
        key = ':'.join([data['jpId'], data['vjId']])
        try:
            handlers[key].send(data)
        except KeyError:
            handlers[key] = init_coroutine(handler, writer)
            handlers[key].send(data)

    # close the handlers (probably unnecessary, but might be good if we end up
    # looping with many input csv files)
    for h in handlers.values():
        h.close()

    try:
        writer.send('CLOSE')
    except StopIteration:
        print('\nDone.')


if __name__ == '__main__':
    process(read_csv('../../data/siri.20121106.csv'))
