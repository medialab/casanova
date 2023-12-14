# =============================================================================
# Casanova Named Record Unit Tests
# =============================================================================
from typing import List, Optional, Tuple, Set, Dict

import json
import pytest
from dataclasses import dataclass

from casanova.record import (
    TabularRecord,
    tabular_field,
    infer_fieldnames,
)
from casanova.ndjson import TabularJSONEncoder


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

        with pytest.raises(TypeError, match="mismatch"):
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

    def test_recursive(self):
        @dataclass
        class Author(TabularRecord):
            name: str
            surname: str

        @dataclass
        class Meta(TabularRecord):
            count: int
            author: Author

        @dataclass
        class Video(TabularRecord):
            title: str
            meta: Meta

        assert Video.fieldnames() == [
            "title",
            "meta_count",
            "meta_author_name",
            "meta_author_surname",
        ]

        video = Video(
            title="test",
            meta=Meta(count=14, author=Author(name="Guillaume", surname="Gilford")),
        )

        assert video.as_csv_row() == ["test", "14", "Guillaume", "Gilford"]
        assert video.as_csv_dict_row() == {
            "title": "test",
            "meta_count": "14",
            "meta_author_name": "Guillaume",
            "meta_author_surname": "Gilford",
        }

        assert Video.parse(["test", "14", "Guillaume", "Gilford"]) == video

    def test_custom_serializer(self):
        @dataclass
        class Video(TabularRecord):
            title: str
            error: Exception = tabular_field(serializer=lambda e: repr(e) + " Success")

        video = Video(title="Test", error=KeyError("k"))

        assert video.as_csv_row() == ["Test", "KeyError('k') Success"]

    def test_custom_json_encoder(self):
        @dataclass
        class Video(TabularRecord):
            title: str
            duration: int

        @dataclass
        class Document(TabularRecord):
            name: str
            videos: List[Video]
            main: Video

        result = json.dumps(
            Document("doc", [Video("one", 14), Video("two", 67)], Video("three", 56)),
            cls=TabularJSONEncoder,
            sort_keys=True,
        )

        assert (
            result
            == '{"main": {"duration": 56, "title": "three"}, "name": "doc", "videos": [{"duration": 14, "title": "one"}, {"duration": 67, "title": "two"}]}'
        )


class TestMiscUtils(object):
    def test_infer_fielndames(self):
        @dataclass
        class Video(TabularRecord):
            title: str
            duration: int

        assert infer_fieldnames(Video("test", 45)) == ["title", "duration"]
        assert infer_fieldnames(["test", "ok"]) == ["col1", "col2"]
        assert infer_fieldnames("test") == ["value"]
        assert infer_fieldnames({1, 2, 3}) is None

        dict_fieldnames = infer_fieldnames({"title": "test", "color": "blue"})

        assert dict_fieldnames is not None

        assert sorted(dict_fieldnames) == [
            "color",
            "title",
        ]
