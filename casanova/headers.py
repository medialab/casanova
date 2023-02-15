# =============================================================================
# Casanova Headers
# =============================================================================
#
# Utility class representing a CSV file's headers
#
from operator import itemgetter


class DictLikeRow(object):
    __slots__ = ("__mapping", "__row")

    def __init__(self, mapping, row):
        self.__mapping = mapping
        self.__row = row

    def __getitem__(self, key):
        return self.__row[self.__mapping[key]]

    def __getattr__(self, key):
        return self.__getitem__(key)


class Headers(object):
    def __init__(self, fieldnames):
        self.__mapping = {h: i for i, h in enumerate(fieldnames)}

    def rename(self, old_name, new_name):
        if old_name == new_name:
            raise TypeError

        self.__mapping[new_name] = self[old_name]
        del self.__mapping[old_name]

    def __len__(self):
        return len(self.__mapping)

    def __getitem__(self, key):
        return self.__mapping[key]

    def __getattr__(self, key):
        return self.__getitem__(key)

    def __contains__(self, key):
        return key in self.__mapping

    def __iter__(self):
        yield from sorted(self.__mapping.items(), key=itemgetter(1))

    def as_dict(self):
        return self.__mapping.copy()

    def get(self, key, default=None):
        return self.__mapping.get(key, default)

    def collect(self, keys):
        return [self[k] for k in keys]

    def wrap(self, row):
        return DictLikeRow(self.__mapping, row)

    def __repr__(self):
        class_name = self.__class__.__name__

        representation = "<" + class_name

        for h, i in self:
            if h.isidentifier():
                representation += " %s=%s" % (h, i)
            else:
                representation += ' "%s"=%s' % (h, i)

        representation += ">"

        return representation
