from collections import defaultdict
import csv
import gzip
import json


class Cfg:
    weather_file = '../../data/weather_output.csv'
    stops_file = '../../data/stopinfo.csv'
    jpId_stops_file = '../../data/routes.txt'


# Useful functions -------------------------------------

def bin_time(dt):
    if dt.hour <= 4:
        return 'early_am'
    elif dt.hour >= 5 and dt.hour <= 12:
        return 'am'
    elif dt.hour >= 13 and dt.hour <= 20:
        return 'pm'
    elif dt.hour >= 21:
        return 'late_pm'
    else:
        raise ValueError('Invalid Hour')


def init_coroutine(coroutine, *args):
    """takes a coroutine and optional parameters to be passed, returns a primed
    instance of the coroutine"""
    c = coroutine() if not args else coroutine(*args)
    next(c)
    return c


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


# functions and constants to get various data from files ---------
# DO NOT IMPORT DIRECTLY. Import constants defined in next section.

def __get_weather_data():
    """Returns the weather data in the format of:
    {DD/MM/YYYY: {TIME_BIN: [cloud, rain, temp, wind]}}
    """
    weather = defaultdict(lambda: defaultdict(list))
    rows = read_csv(Cfg.weather_file)
    next(rows)  # first line is the column headers
    for row in rows:
        weather[row[0]][row[1]] = row[2:]

    return weather


def __get_stop_coords():
    """Returns a dict of {str(stopId) : [str(lat), str(lon)]}"""
    stops = read_csv(Cfg.stops_file)

    # dict keys of the stopId, value is [lat, lon] list
    d = defaultdict(list)

    for r in stops:
        d[str(r[4])] = [str(r[7]), str(r[8])]

    return d


def __get_journey_pattern_id_stops():
    """Returns a dict of {str(vjId): [ str(stopId) ]"""
    with open(Cfg.jpId_stops_file) as f:
        routes = json.load(f)
        d = defaultdict(list)
        for jpId, v in routes.items():
            d[str(jpId)] = [str(stopId) for stopId in v['stops']]
    return d


# Constants for easy imports ------------------------------
WEATHER_DATA = __get_weather_data()
STOP_COORDS = __get_stop_coords()
JOURNEY_PATTERN_ID_STOPS = __get_journey_pattern_id_stops()
