from casanova import RowWrapper


def multiply_by_10(x):
    return x * 10


def multiply(index: int, row: RowWrapper):
    return multiply_by_10(int(row.n))
