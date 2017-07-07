from collections import defaultdict
import json

from processing_tools import read_csv


stop_table = read_csv('../data/stopinfo.csv')
route_data_file = open('../data/routes.txt')
rd = json.load(route_data_file)

# dict keys of the stopId, value is [lat, lon] list
STOP_COORDS = defaultdict(list)

for r in stop_table:
    STOP_COORDS[str(r[4])] = [r[7], r[8]]
