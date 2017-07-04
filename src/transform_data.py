from collections import namedtuple, defaultdict
import csv
import gzip
from os import mkdir
from os.path import isdir
import json
from pprint import pprint

from geopy.distance import distance as dist

from filereader import read_csv


class Cfg:
	bus_data_file = '../../../data/siri.20130101.csv'
	stops_data_file = '../../../stopinfo.csv'
	bus_stop_radius = 100  # meters. Arbitraliy chosen.
	out_dir = '../data/out'
	out_file = 'out.csv'
	pick_route = '00010001'


# Using the namedtuple makes the code more readable, but we aren't using it a lot
# here, so it may not be worth the overhead
BusRecord = namedtuple('BusRecord',
                       ['timestamp', 'lineId', 'direction', 'journeyPatternId',
                        'timeFrame', 'vehicleJourneyId', 'operator', 'conjestion',
                        'lon', 'lat', 'delay', 'blockId', 'vehicleId', 'stopId',
                        'atStop'])

StopRecord = namedtuple('StopRecord', ['number', 'name', 'locality', 'locality_num', 'code', 'stop_no', 'n_class', 'n_id', 'easting', 'northing', 'lat', 'lon'])


def main():
	bus_data = read_csv(Cfg.bus_data_file, BusRecord)
	stop_data = list(read_csv(Cfg.stops_data_file, StopRecord))
	with open('../../../routes.txt') as data_file:
		data = json.load(data_file)


	routes = defaultdict(list)
	count = 0

	# for x in stop_data:
	# 	print(x.stop_no)

	# very inefficient, to run this for the entire dataset will take > a day
	for bus in bus_data:
		for stop in stop_data:
			if dist((bus.lat, bus.lon), (stop.lat, stop.lon)).meters < Cfg.bus_stop_radius:
				print('\n{} : {}'.format(stop.name, stop.id))

				new_row = (bus.vehicleId, bus.lineId, bus.timestamp,
                   	       stop.id, stop.name, stop.lat, stop.lon)

				routes[bus.vehicleJourneyId].append(new_row)
		count += 1
		print('\r{} rows processed'.format(count), end='')

	# if not isdir(Cfg.out_dir):
	# 	mkdir(Cfg.out_dir)

	# Maybe incorporate this code into the block above?
	# for k, v in routes.items():
	# 	filename = k + '.csv'
	# 	with open(filename, 'wt') as f:
	# 		writer = csv.writer(f)
	# 		for row in v:
	# 			writer.writerow(row)


if __name__ == '__main__':
    main()
