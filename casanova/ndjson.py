from typing import Any

import json
import dataclasses


class TabularJSONEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


class writer(object):
    def __init__(self, f):
        self.__file = f

    def writerow(self, row):
        self.__file.write(
            json.dumps(row, ensure_ascii=False, allow_nan=False, cls=TabularJSONEncoder)
        )
        self.__file.write("\n")


class reader(object):
    def __init__(self, f):
        self.__file = f

    def __next__(self):
        line = ""

        while not line:
            line = next(self.__file).strip()

        return json.loads(line)
