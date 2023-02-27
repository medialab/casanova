# =============================================================================
# Casanova Serialization Unit Tests
# =============================================================================
from pytest import raises

from casanova.serialization import CSVSerializer


class TestSerialization(object):
    def test_basics(self):
        serializer = CSVSerializer()

        assert serializer("test") == "test"
        assert serializer(None) == ""
        assert serializer(None, none_value="null") == "null"
        assert serializer(None, stringify_everything=False) == None
        assert serializer(True) == "true"
        assert serializer(False) == "false"
        assert serializer(True, true_value="yes") == "yes"
        assert serializer(False, false_value="") == ""
        assert serializer(45) == "45"
        assert serializer(7.4) == "7.4"
        assert serializer(45, stringify_everything=False) == 45
        assert serializer(7.4, stringify_everything=False) == 7.4
        assert serializer(["blue", "yellow"]) == "blue|yellow"
        assert serializer(["blue", "yellow"], plural_separator="&") == "blue&yellow"

        with raises(NotImplementedError):
            serializer({"hello": 45})

        assert serializer({"hello": 45}, as_json=True) == '{"hello": 45}'

    def test_serialize_row(self):
        serializer = CSVSerializer(plural_separator="#")

        assert serializer.serialize_row(["one", ("a", "b"), 45]) == ["one", "a#b", "45"]

    def test_serialize_dict_row(self):
        serializer = CSVSerializer()

        assert serializer.serialize_dict_row(
            {"title": "one", "not-include": False}, fieldnames=["title"]
        ) == ["one"]
