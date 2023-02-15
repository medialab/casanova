# =============================================================================
# Casanova Headers Unit Tests
# =============================================================================
from casanova.headers import (
    parse_selection,
    SimpleSelection,
    RangeSelection,
    IndexedSelection,
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
            SimpleSelection(key="Date - Opening"),
            SimpleSelection(key="Date - Actual Closing"),
            RangeSelection(start="Header1", end="Header2"),
            IndexedSelection(key="Foo", index=3),
            SimpleSelection(key="Header, Whatever"),
            RangeSelection(start=0, end=3),
            RangeSelection(start=2, end=None),
            SimpleSelection(key=2),
            RangeSelection(start=8, end=4),
        ]

        selection = list(parse_selection("!1-4"))

        assert selection == [RangeSelection(start=0, end=3, negative=True)]

        indices = headers.select("1-4")

        assert indices == [0, 1, 2]

        indices = headers.select('Header2,1-4,6-4,"Date - Opening",1-1,10-')

        assert indices == [1, 0, 1, 2, 5, 4, 8, 0, 9, 10]
