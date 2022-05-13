# =============================================================================
# Casanova K-Way Reader Unit Tests
# =============================================================================
import os
import csv
import casanova


def get_files():
    resources_path = './test/resources/'
    file_names = ["lunch", "dinner", "party"]
    file_paths = [os.path.join(resources_path, 'guests_{}_sorted.csv'.format(fn)) for fn in file_names]
    return file_names, file_paths


class TestKWayMerge(object):

    def test_column_id(self):
        file_names, file_paths = get_files()
        files = [open(file_path) for file_path in file_paths]

        readers = {fn: casanova.reader(f) for fn, f in zip(file_names, files)}
        guest_name_pos = 0

        result = [item for item in casanova.kway_merge(readers, guest_name_pos)]

        for file in files:
            file.close()

        assert result == [
            (["Alexander", "0"], "dinner"),
            (["Alexander", "0"], "party"),
            (["Gary", "1"], "lunch"),
            (["Gary", "1"], "dinner"),
            (["Gary", "1"], "party"),
            (["John", "2"], "party"),
            (["Lisa", "3"], "lunch"),
            (["Lisa", "3"], "party")
        ]

    def test_comparison_keys(self):
        file_names, file_paths = get_files()
        files = [open(file_path) for file_path in file_paths]

        readers = {fn: casanova.reader(f) for fn, f in zip(file_names, files)}
        guest_number_pos = 1
        comparison_keys = {fn: lambda row: int(row[guest_number_pos]) for fn in file_names}

        result = [item for item in casanova.kway_merge(readers, comparison_keys)]

        for file in files:
            file.close()

        assert result == [
            (["Alexander", "0"], "dinner"),
            (["Alexander", "0"], "party"),
            (["Gary", "1"], "lunch"),
            (["Gary", "1"], "dinner"),
            (["Gary", "1"], "party"),
            (["John", "2"], "party"),
            (["Lisa", "3"], "lunch"),
            (["Lisa", "3"], "party")
        ]

    def test_csv_reader(self):
        file_names, file_paths = get_files()
        files = [open(file_path) for file_path in file_paths]

        readers = {fn: csv.reader(f) for fn, f in zip(file_names, files)}
        # skip title
        for reader in readers.values():
            next(reader)

        guest_number_pos = 1
        comparison_keys = {fn: lambda row: int(row[guest_number_pos]) for fn in file_names}

        result = [item for item in casanova.kway_merge(readers, comparison_keys)]

        for file in files:
            file.close()

        assert result == [
            (["Alexander", "0"], "dinner"),
            (["Alexander", "0"], "party"),
            (["Gary", "1"], "lunch"),
            (["Gary", "1"], "dinner"),
            (["Gary", "1"], "party"),
            (["John", "2"], "party"),
            (["Lisa", "3"], "lunch"),
            (["Lisa", "3"], "party")
        ]
