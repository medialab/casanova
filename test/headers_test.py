# =============================================================================
# Casanova Headers Unit Tests
# =============================================================================
import pytest

from casanova.headers import (
    parse_selection,
    SingleColumn,
    ColumnRange,
    IndexedColumn,
    Headers,
)
from casanova.exceptions import InvalidSelectionError


class TestHeaders(object):
    def test_duplicate_field(self):
        headers = Headers(["Foo", "Foo", "Nope", "Foo"])

        assert headers.get("Foo") == 0
        assert headers.get("Foo", index=2) == 3

    def test_selection_dsl(self):
        headers = Headers(
            [
                "Header1",
                "Header2",
                "Header3",
                "Header4",
                "Foo",
                "Foo",
                "Header5",
                "Foo",
                "Date - Opening",
                "Date - Actual Closing",
                "Header, Whatever",
            ]
        )

        selection = list(
            parse_selection(
                '"Date - Opening","Date - Actual Closing",Header1-Header2,Foo[3],"Header, Whatever",1-4,3-,2,9-5'
            )
        )

        assert selection == [
            SingleColumn(key="Date - Opening"),
            SingleColumn(key="Date - Actual Closing"),
            ColumnRange(start="Header1", end="Header2"),
            IndexedColumn(key="Foo", index=3),
            SingleColumn(key="Header, Whatever"),
            ColumnRange(start=0, end=3),
            ColumnRange(start=2, end=None),
            SingleColumn(key=1),
            ColumnRange(start=8, end=4),
        ]

        selection = parse_selection("!1-4")

        assert selection.inverted
        assert list(selection) == [ColumnRange(start=0, end=3)]

        indices = headers.select("1-4")

        assert indices == [0, 1, 2, 3]

        indices = headers.select('Header2,1-4,6-4,"Date - Opening",1-1,10-')

        assert indices == [1, 0, 1, 2, 3, 5, 4, 3, 8, 0, 9, 10]

        indices = headers.select("!Header2")

        assert indices == [0, 2, 3, 4, 5, 6, 7, 8, 9, 10]

        indices = headers.select("!Foo[1]")

        assert headers.select("Foo[1]") == [5]
        assert indices == [0, 1, 2, 3, 4, 6, 7, 8, 9, 10]

        indices = headers.select("!2-6")

        assert headers.select("2-6") == [1, 2, 3, 4, 5]
        assert indices == [0, 6, 7, 8, 9, 10]

        selection = list(parse_selection('\\"Hey\\"'))

        assert selection == [SingleColumn(key='"Hey"')]

        selection = list(parse_selection('"Date - \\"Opening"'))

        assert selection == [SingleColumn(key='Date - "Opening')]

        assert parse_selection("3").is_suitable_without_headers()
        assert not parse_selection("Header1").is_suitable_without_headers()

        with pytest.raises(InvalidSelectionError, match="no-headers"):
            Headers.select_no_headers(3, "Header1")

        with pytest.raises(InvalidSelectionError, match="index"):
            Headers.select_no_headers(2, "6")

        assert Headers.select_no_headers(5, "1-4") == [0, 1, 2, 3]

    def test_projection(self):
        headers = Headers(["name", "surname", "age", "height"])
        row = ["John", "Williams", "45", "190"]

        p = headers.project(
            {"name": "name", "surname": 1, "numbers": ["age", "height"]}
        )

        assert p(row) == {
            "name": "John",
            "surname": "Williams",
            "numbers": ["45", "190"],
        }

        p = headers.project(["age", ("name", "surname")])

        assert p(row) == ["45", ("John", "Williams")]

        p = headers.project("age")

        assert p(row) == "45"
