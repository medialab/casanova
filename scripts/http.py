import casanova

with casanova.reader(
    "https://raw.githubusercontent.com/medialab/corpora/master/polarisation/medias.csv"
) as reader:
    for name in reader.cells("name"):
        print(name)
