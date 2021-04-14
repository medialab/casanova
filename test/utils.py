import csv
from io import StringIO


def collect_csv(buf):
    if isinstance(buf, StringIO):
        return list(line for line in csv.reader(StringIO(buf.getvalue())))

    with open(buf) as f:
        return list(csv.reader(f))
