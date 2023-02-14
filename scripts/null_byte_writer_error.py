import io
import csv

value = "my\x00string"

with io.StringIO() as buf:
    writer = csv.writer(buf)
    writer.writerow([value])
