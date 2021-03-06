from collections import namedtuple
import csv
from glob import glob
import gzip
from os import mkdir, remove
from os.path import exists, expanduser, isdir

from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from filereader import read_csv


class Cfg:
    files = glob(expanduser('~') + '/datasets/BusData/*.gz')
    db_server = 'sqlite:///'
    db_location = expanduser('~') + '/db/'
    db_name = 'db.db'
    # the number of records to be written to the database at once.
    batch_size = 10000


Base = declarative_base()


class BusData(Base):
    __tablename__ = 'BusData'
    id = Column(Integer, primary_key=True)
    timestamp = Column(String(26))
    lineId = Column(String(10))
    direction = Column(Integer)
    journeyPatternId = Column(Integer)
    timeFrame = Column(String(24))
    vehicleJourneyId = Column(Integer)
    operator = Column(String(10))
    conjestion = Column(Integer)
    lon = Column(String(30))
    lat = Column(String(30))
    delay = Column(String(30))
    blockId = Column(String(30))
    vehicleId = Column(String(30))
    stopId = Column(String(30))
    atStop = Column(Integer)


Row = namedtuple('Row', ['timestamp', 'lineId', 'direction',
                         'journeyPatternId', 'timeFrame', 'vehicleJourneyId', 'operator',
                         'conjestion', 'lon', 'lat', 'delay', 'blockId', 'vehicleId', 'stopId',
                         'atStop'])


def batch(iterable, batch_size):
    batch = []
    for item in iterable:
        if len(batch) == batch_size:
            yield batch
            batch = []
        batch.append(item)
    yield batch


def main():
    if exists(Cfg.db_location + Cfg.db_name):
        s = 'Database already exists and will be overwritten.\nEnter "y" to continue: '
        if input(s) != 'y':
            raise SystemExit

        remove(Cfg.db_location + Cfg.db_name)

    if not isdir(Cfg.db_location):
        mkdir(Cfg.db_location)

    # TODO: THIS IS BROKEN. NEEDS FIXING. CONSIDER USING ITERTOOLS.CHAIN
    rows = (yield next(read_csv(fname, Row)) for fname in Cfg.files)

    records = (BusData(
        timestamp=item.timestamp,
        lineId=item.lineId,
        direction=item.direction,
        journeyPatternId=item.journeyPatternId,
        timeFrame=item.timeFrame,
        vehicleJourneyId=item.vehicleJourneyId,
        operator=item.operator,
        conjestion=item.conjestion,
        lon=item.lon,
        lat=item.lat,
        delay=item.delay,
        blockId=item.blockId,
        vehicleId=item.vehicleId,
        stopId=item.stopId,
        atStop=item.atStop
    ) for item in rows)

    # batch size chosen arbitrarily. What's the maximim we can use?
    batches = batch(records, Cfg.batch_size)

    engine = create_engine(Cfg.db_server + Cfg.db_location + Cfg.db_name)
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()

    count = 0
    for b in batches:
        session.bulk_save_objects(b)
        count += len(b)
        print('\r{} rows written to table'.format(count), end='')

    print('Committing session...')
    # not sure if this is neccessary when using bulk_save, but it doesn't break
    # anything so i left it in for now
    session.commit()
    print('Done')


if __name__ == '__main__':
    main()
