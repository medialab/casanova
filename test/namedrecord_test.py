# =============================================================================
# Casanova Named Record Unit Tests
# =============================================================================
import pytest
from collections import OrderedDict

from casanova import namedrecord, temporary_defaults


class TestNamedRecord(object):
    def test_basics(self):
        Record = namedrecord("Record", ["x", "y"])

        r = Record(x=34, y=22)

        assert len(r) == 2
        assert list(r) == [34, 22]
        assert r[0] == 34
        assert r.x == 34
        assert r["x"] == 34

        with pytest.raises(KeyError):
            r["z"]

        assert r.get("x") == 34
        assert r.get(0) == 34
        assert r.get(54) is None
        assert r.get("z") is None

        Video = namedrecord("Video", ["title", "has_captions", "tags"])

        v = Video("Super video", True, ["film", "pop"])

        assert v.as_csv_row() == ["Super video", "true", "film|pop"]
        assert v.as_dict() == {
            "title": "Super video",
            "has_captions": True,
            "tags": ["film", "pop"],
        }

        assert Video.fieldnames == ["title", "has_captions", "tags"]

        v = Video(*["Title", False, []])

        assert v.as_dict() == {"title": "Title", "has_captions": False, "tags": []}

    def test_defaults(self):
        Record = namedrecord("Record", ["x", "y", "z"], defaults=[20, 30])

        r = Record(27)

        assert r == Record(27, 20, 30)

        assert list(Record(10, z=45)) == [10, 20, 45]

    def test_non_str_plurals(self):
        Record = namedrecord("Record", ["title", "positions"], plural=["positions"])

        r = Record("Hello", positions=list(range(3)))

        assert r.as_csv_row() == ["Hello", "0|1|2"]

    def test_json_serialization(self):
        Record = namedrecord("Record", ["title", "data"], json=["data"])

        r = Record("Hello", {"one": [0, 1]})

        assert r.as_csv_row() == ["Hello", '{"one": [0, 1]}']
        assert r.as_csv_dict_row() == OrderedDict(title="Hello", data='{"one": [0, 1]}')

        r = Record("Test", None)

        assert r.as_csv_row() == ["Test", "null"]

    def test_set_is_plural(self):
        Video = namedrecord(
            "Video",
            ["title", "has_captions", "tags"],
            boolean=["has_captions"],
            plural=["tags"],
        )

        v = Video("Super video", True, {"film", "pop"})

        assert v.as_csv_row() == [
            "Super video",
            "true",
            "film|pop",
        ] or v.as_csv_row() == ["Super video", "true", "pop|film"]
        assert v.as_dict() == {
            "title": "Super video",
            "has_captions": True,
            "tags": {"film", "pop"},
        }

        assert Video.fieldnames == ["title", "has_captions", "tags"]

    def test_formatting_options(self):
        Video = namedrecord(
            "Video",
            ["title", "has_captions", "has_info", "tags", "category"],
            boolean=["has_captions", "has_info"],
            plural=["tags"],
            defaults=[None],
        )

        v = Video("Title", False, True, ["film", "pop"])

        assert v.as_csv_row(
            plural_separator="#", none_value="none", true_value="yes", false_value="no"
        ) == ["Title", "no", "yes", "film#pop", "none"]

    def test_formatting_options_constructor(self):
        Video = namedrecord(
            "Video",
            ["title", "has_captions", "has_info", "tags", "category"],
            boolean=["has_captions", "has_info"],
            plural=["tags"],
            defaults=[None],
            plural_separator="#",
            none_value="none",
            true_value="yes",
            false_value="no",
        )

        v = Video("Title", False, True, ["film", "pop"])

        assert v.as_csv_row(
            plural_separator="#", none_value="none", true_value="yes", false_value="no"
        ) == ["Title", "no", "yes", "film#pop", "none"]

    def test_formatting_defaults(self):
        Video = namedrecord(
            "Video",
            ["title", "has_captions", "has_info", "tags", "category"],
            boolean=["has_captions", "has_info"],
            plural=["tags"],
            defaults=[None],
        )

        v = Video("Title", False, True, ["film", "pop"])

        with temporary_defaults(
            plural_separator="#", none_value="none", true_value="yes", false_value="no"
        ):
            assert v.as_csv_row() == ["Title", "no", "yes", "film#pop", "none"]

        assert v.as_csv_row() == ["Title", "false", "true", "film|pop", ""]
