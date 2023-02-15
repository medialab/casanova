import io
import csv

value = "my\x00string"

try:
    with io.StringIO() as buf:
        writer = csv.writer(buf)
        writer.writerow([value])
except csv.Error as e:
    print(str(e))
