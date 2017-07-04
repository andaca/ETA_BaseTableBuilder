import json
from pprint import pprint
import csv
from geopy.distance import distance as dist
import pandas as pd

# def reencode(file):
# 		yield file.decode('latin-1').encode('utf-8')
#
# stop_table = csv.reader(reencode(open('../../../stopinfo.csv')), delimiter=",", quotechar='"')

b = open('../data/siri.20121106.csv')
s = open('../data/stopinfo.csv')
bus_table = csv.reader(b)
stop_table = csv.reader(s)

# df = pd.read_csv(bus_table)
# df = df[pd.notnull(df[3])]


with open('../data/routes.txt') as data_file:
    route_data = json.load(data_file)

# BusRecord = namedtuple('BusRecord',
#                        ['timestamp', 'lineId', 'direction', 'journeyPatternId',
#                         'timeFrame', 'vehicleJourneyId', 'operator', 'conjestion',
#                         'lon', 'lat', 'delay', 'blockId', 'vehicleId', 'stopId',
#                         'atStop'])


# StopRecord = namedtuple('StopRecord', ['number', 'name', 'locality', 'locality_num', 'code', 'stop_no', 'n_class', 'n_id', 'easting', 'northing', 'lat', 'lon'])
b_time, b_line_id, b_direction, b_jour_patt_id, b_timeframe, b_veh_jour_id, b_oper, b_conj, b_lon, b_lat, b_delay, b_block_id, b_veh_id, b_stop_id, b_at_stop = 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14

s_num, s_name, s_locality, s_locality_num, s_stop_no, s_easting, s_northing, s_lat, s_lon = 0, 1, 2, 3, 4, 5, 6, 7, 8


for row in bus_table:
    if row[b_jour_patt_id] != 'null' and row[b_jour_patt_id] != '':
        print("J_ID is", row[b_jour_patt_id])
        scheduled_stops = route_data[row[b_jour_patt_id]]['stops']
        for stops_row in stop_table:
            if int(stops_row[s_stop_no]) in scheduled_stops:
                print('Found', stops_row[s_stop_no], 'at lat:',
                      stops_row[s_lat], ', lon:', stops_row[s_lon])


# pprint(route_data)

s.close()
b.close()
