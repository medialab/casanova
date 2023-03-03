# =============================================================================
# Casanova Named Record Unit Tests
# =============================================================================
from typing import List, Optional, Tuple, Set, Dict

import pytest
from collections import OrderedDict
from dataclasses import dataclass

from casanova.namedrecord import namedrecord, TabularRecord, tabular_field


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


class TestTabularRecord(object):
    def test_basics(self):
        @dataclass
        class Video(TabularRecord):
            title: str
            duration: int
            tags: List[str] = tabular_field(plural_separator="&", default_factory=list)

        video = Video(title="The Movie", duration=180, tags=["action", "romance"])

        assert video.as_csv_row() == ["The Movie", "180", "action&romance"]
        assert video.as_csv_dict_row() == {
            "title": "The Movie",
            "duration": "180",
            "tags": "action&romance",
        }

        assert Video.fieldnames() == ["title", "duration", "tags"]
        assert Video.fieldnames(prefix="video_") == [
            "video_title",
            "video_duration",
            "video_tags",
        ]

    def test_parse(self):
        @dataclass
        class Video(TabularRecord):
            title: str
            description: str
            subtitle: Optional[str]
            duration: int
            time: float
            seen: bool
            tags: List[str]
            tuple_tags: Tuple[str, str]
            set_tags: Set[str] = tabular_field(plural_separator="&")
            good: bool = tabular_field(true_value="yes")

        with pytest.raises(TypeError, match="wrong number"):
            Video.parse(["title"])

        video = Video.parse(
            ["Title", "", "", "167", "45.6", "false", "a|b", "b|c", "c&d", "yes"]
        )

        assert video == Video(
            title="Title",
            description="",
            subtitle=None,
            duration=167,
            time=45.6,
            seen=False,
            tags=["a", "b"],
            tuple_tags=("b", "c"),
            set_tags={"d", "c"},
            good=True,
        )

    def test_json(self):
        @dataclass
        class Video(TabularRecord):
            title: str
            data: Dict[str, str] = tabular_field(as_json=True)

        video = Video("Title", {"hello": "world"})

        assert video.as_csv_row() == ["Title", '{"hello": "world"}']

        video = Video.parse(["Other", '{"test": "coucou"}'])

        assert video == Video("Other", {"test": "coucou"})
