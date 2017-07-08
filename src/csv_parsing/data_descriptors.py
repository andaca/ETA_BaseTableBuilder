from collections import defaultdict
from os.path import expanduser
import json

from data_utils import read_csv


class Cfg:
    weather_file = expanduser('~/datasets/datafiles/weather_output.csv')
    stops_file = expanduser('~/datasets/datafiles/stopinfo.csv')
    jpId_stops_file = expanduser('~/datasets/datafiles/routes.txt')


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
        d[str(r[4])] = (str(r[7]), str(r[8]))

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


def get_stops(data):
    """returns stops pertaining to the journeyPatternId specified in the data dict"""
    global STOP_COORDS
    global JOURNEY_PATTERN_ID_STOPS

    stops = JOURNEY_PATTERN_ID_STOPS[data['jpId']]
    coords = [STOP_COORDS[stop] for stop in stops]
    return dict(zip(stops, coords))
