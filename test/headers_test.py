# =============================================================================
# Casanova Headers Unit Tests
# =============================================================================
from casanova.headers import (
    parse_selection,
    SingleColumn,
    ColumnRange,
    IndexedColumn,
    Headers,
)


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
            SingleColumn(key=2),
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
