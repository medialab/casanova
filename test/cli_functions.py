from casanova import RowWrapper


def multiply_by_10(x):
    return x * 10


def main(row: RowWrapper):
    return multiply_by_10(int(row.n))


def enumerate_times_20(index: int):
    return index * 20


def gen():
    yield 1
    yield 2


def plus_5(n: str) -> int:
    return int(n) + 5


def concat_name(name: str, surname: str) -> str:
    return name + "%" + surname
