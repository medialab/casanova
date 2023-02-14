import csv
from file_read_backwards import FileReadBackwards

with FileReadBackwards("./test/resources/people.csv", encoding="utf-8") as f:
    for line in csv.reader(f):
        print(line)
        break
