# =============================================================================
# Casanova K-Way Merge
# =============================================================================
#
# Algorithm reading k sorted csv files and merging them into a single sorted csv
# output.
# This is useful when the files do not fit in memory.

from heapq import merge


def kway_merge(readers, key):
    """
    Merge multiple casanova readers into a single sorted output.

    Args:
        readers (dict): dictionary of the form {file_id: casanova.reader} or
            {file_id: csv.reader}. The file_id keys allow to identify the
            origin files.
        key (int or dict): comparison key used to sort rows. If an integer is provided, it
            is treated as a column index, and all readers will be sorted on that column.
            If a dict is provided, it should contain the file_id as keys and a key function
            (https://docs.python.org/3/glossary.html#term-key-function) as values.

    Returns:
        a sorted iterator of (row, file_id) tuples
    """

    if type(key) == dict:

        def comparison_key(tuple):
            row, file_id = tuple
            return key[file_id](row)

    elif type(key) == int:
        def comparison_key(tuple):
            row, file_id = tuple
            return row[key]

    else:
        raise TypeError('key must be an integer or a dictionary')

    def iterate_on_rows(reader, file_id):
        for row in reader:
            yield row, file_id

    for row, file_id in merge(*(iterate_on_rows(v, k) for k, v in readers.items()), key=comparison_key):
        yield row, file_id
