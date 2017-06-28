import csv
import gzip


def read_csv(filename, named_tuple=None):
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
