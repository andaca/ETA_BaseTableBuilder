from datetime import datetime
from os.path import expanduser

from geopy.distance import distance

from data_utils import read_csv, coroutine, coro_csv_writer, coro_print
from data_descriptors import bin_time, WEATHER_DATA, get_stops


def get_weather(data, weather):
    """Takes a dict representing a row, looks up the relevant weather information.
    @args: data (dict), weather (dict)
    @returns: {'cloud':str? , 'rain':str?, 'temp':str?, 'wind':str?}
    """

    t_bin = bin_time(data['dt'])
    date = data['dt'].strftime('%d/%m/%Y')

    return dict(zip(['cloud', 'rain', 'temp', 'wind'],
                    weather[date][t_bin]))


def nearest_stop(data, stops, max_dist=100):
    """takes a dictionary, finds the closest stop contained in stops.
    @args: data (dict), stops {stopId: (lat, lon)} max_dist (int) *optional. In meters*
    @returns: dict
    """
    nearest = {'nearestStop': None, 'nearestDistance': float('inf')}

    for stop, coords in stops.items():
        dist = distance(coords, (data['lat'], data['lon'])).meters

        if dist <= max_dist and dist < nearest['nearestDistance']:
            nearest = {'nearestStop': stop, 'nearestDistance': round(dist, 2)}

    return nearest


def rowify(data):
    """Takes a data dictionary, and returns a list of the data contained within
    it as it should be written to a csv file."""

    columns = ['dt', 'line', 'jpId', 'timeframe', 'vjId', 'lat', 'lon', 'delay',
               'blockId', 'stopId', 'atStop', 'running_dist', 'running_time',
               'nearestStop', 'nearestDistance', 'cloud', 'rain', 'temp', 'wind']

    return [data[c] for c in columns]


@coroutine
def journey_parser(writer):
    """Expects to receive data for a single journey. Calculates running distance
    and running time. Writes rows to writer."""

    prev = yield

    running_dist = 0
    running_time = 0
    d = {**prev, **{'running_dist': 0, 'running_time': 0}}
    writer.send(rowify(d))

    while True:
        data = yield
        running_dist += distance((prev['lat'], prev['lon']),
                                 (data['lat'], data['lon'])).meters

        running_time += (data['dt'] - prev['dt']).seconds

        if data['nearestStop'] is not None:
            writer.send(rowify({**data, **{'running_dist': running_dist,
                                           'running_time': running_time}}))

        prev = data


def main():
    rows = read_csv(expanduser('~/datasets/datafiles/siri.20121106.csv'))

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

    # A nifty way of merging dictionaries. Python3.5+
    datas = ({**d, **get_weather(d, WEATHER_DATA)}
             for d in datas)

    datas = ({**d, **nearest_stop(d, get_stops(d))}
             for d in datas)

    # writer = coro_print()  # prints to screen for debugging.
    writer = coro_csv_writer(expanduser(
        '~/datasets/datafiles/busdata_improved.csv'))

    columns = ['dt', 'line', 'jpId', 'timeframe', 'vjId', 'lat', 'lon', 'delay',
               'blockId', 'stopId', 'atStop', 'running_dist', 'running_time',
               'closest_stop', 'closest_stop_dist', 'cloud', 'rain', 'temp', 'wind']
    writer.send(columns)  # write column headers

    # seperate datas by journeyPatternId, vehicleJourneyId and timeframe
    journey_parsers = {}
    for d in datas:
        key = ':'.join([d['jpId'], d['vjId'], d['timeframe']])
        try:
            journey_parsers[key].send(d)
        except KeyError:
            journey_parsers[key] = journey_parser(writer)
            journey_parsers[key].send(d)

        # TODO: Clean up handlers. Remove old ones once the journey has ended.

    try:
        writer.send(StopIteration)
    except StopIteration:
        print('Done')


if __name__ == '__main__':
    main()
