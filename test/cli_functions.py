from casanova import RowWrapper


def multiply_by_10(x):
    return x * 10


def main(row: RowWrapper):
    return multiply_by_10(int(row.n))


def enumerate_times_20(index: int):
    return index * 20
