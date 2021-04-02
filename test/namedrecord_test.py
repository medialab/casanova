# =============================================================================
# Casanova Named Record Unit Tests
# =============================================================================
import pytest

from casanova import namedrecord


class TestNamedRecord(object):
    def test_basics(self):
        Record = namedrecord('Record', ['x', 'y'])

        r = Record(x=34, y=22)

        assert len(r) == 2
        assert list(r) == [34, 22]
        assert r[0] == 34
        assert r.x == 34
        assert r['x'] == 34

        with pytest.raises(KeyError):
            r['z']

        assert r.get('x') == 34
        assert r.get(0) == 34
        assert r.get(54) is None
        assert r.get('z') is None

        Video = namedrecord(
            'Video',
            ['title', 'has_captions', 'tags'],
            boolean=['has_captions'],
            plural=['tags']
        )

        v = Video('Super video', True, ['film', 'pop'])

        assert v.as_csv_row() == ['Super video', 'true', 'film|pop']
        assert v.as_dict() == {
            'title': 'Super video',
            'has_captions': True,
            'tags': ['film', 'pop']
        }

    def test_defaults(self):
        Record = namedrecord(
            'Record',
            ['x', 'y', 'z'],
            defaults=[20, 30]
        )

        r = Record(27)

        assert r == Record(27, 20, 30)

        assert list(Record(10, z=45)) == [10, 20, 45]
