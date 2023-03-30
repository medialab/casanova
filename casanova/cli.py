from typing import Optional, List

import re
import sys
import math
import random
from os.path import join
from urllib.parse import urlsplit, urljoin
from multiprocessing import Pool as MultiProcessPool
from dataclasses import dataclass

from casanova import Reader, Enricher, CSVSerializer, RowWrapper, Headers
from casanova.utils import import_function


@dataclass
class InitializerOptions:
    code: str
    module: bool
    row_len: int
    args: List[str]
    init_codes: List[str]
    before_codes: List[str]
    after_codes: List[str]
    fieldnames: Optional[List[str]] = None


# NOTE: just a thin wrapper to make sure we catch KeyboardInterrupt in
# child processes gracefully.
class WorkerWrapper(object):
    __slots__ = ("fn",)

    def __init__(self, fn):
        self.fn = fn

    def __call__(self, *args, **kwargs):
        try:
            return self.fn(*args, **kwargs)
        except KeyboardInterrupt:
            sys.exit(1)


class SingleProcessPool(object):
    def imap(self, worker, tasks, chunksize=1):
        for t in tasks:
            yield worker(t)

    def imap_unordered(self, *args, **kwargs):
        yield from self.imap(*args, **kwargs)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return


def get_pool(n: int, options: InitializerOptions):
    initargs = (options,)

    if n < 2:
        multiprocessed_initializer(*initargs)
        return SingleProcessPool()

    return MultiProcessPool(
        n, initializer=multiprocessed_initializer, initargs=initargs
    )


serialize = CSVSerializer()

CODE = None
FUNCTION = None
ARGS = None
BEFORE_CODES = []
AFTER_CODES = []
LOCAL_CONTEXT = {
    # lib
    "join": join,
    "math": math,
    "random": random,
    "re": re,
    "urljoin": urljoin,
    "urlsplit": urlsplit,
    # state
    "fieldnames": None,
    "headers": None,
    "index": 0,
    "row": None,
}
ROW = None


def multiprocessed_initializer(options: InitializerOptions):
    global CODE
    global FUNCTION
    global ARGS
    global BEFORE_CODES
    global AFTER_CODES
    global ROW

    if options.module:
        FUNCTION = import_function(options.code)
        ARGS = options.args
    else:
        CODE = options.code
        BEFORE_CODES = options.before_codes
        AFTER_CODES = options.after_codes

    if options.fieldnames is not None:
        LOCAL_CONTEXT["fieldnames"] = options.fieldnames
        LOCAL_CONTEXT["headers"] = Headers(options.fieldnames)
        headers = LOCAL_CONTEXT["headers"]
    else:
        headers = Headers(range(options.row_len))

    for init_code in options.init_codes:
        exec(init_code, None, LOCAL_CONTEXT)

    LOCAL_CONTEXT["row"] = RowWrapper(headers, None)
    ROW = LOCAL_CONTEXT["row"]


def multiprocessed_worker_using_eval(payload):
    global LOCAL_CONTEXT

    i, row = payload
    LOCAL_CONTEXT["index"] = i
    ROW._replace(row)

    for before_code in BEFORE_CODES:
        exec(before_code, None, LOCAL_CONTEXT)

    value = eval(CODE, None, LOCAL_CONTEXT)

    for after_code in AFTER_CODES:
        exec(after_code, None, LOCAL_CONTEXT)

    return i, value


def collect_args(i):
    for arg_name in ARGS:
        if arg_name == "row":
            yield ROW
        elif arg_name == "index":
            yield i
        elif arg_name == "fieldnames":
            yield LOCAL_CONTEXT["fieldnames"]
        elif arg_name == "headers":
            yield LOCAL_CONTEXT["headers"]


def multiprocessed_worker_using_function(payload):
    i, row = payload
    ROW._replace(row)

    args = tuple(collect_args(i))

    value = FUNCTION(*args)

    return i, value


# TODO: flatmap, reduce?
# TODO: generator functions cast as list and flatmap relation
# TODO: --plural-separator etc.,
# TODO: flag to ignore errors
# TODO: cell selector as value
def mp_iteration(cli_args, reader: Reader):
    worker = WorkerWrapper(
        multiprocessed_worker_using_eval
        if not cli_args.module
        else multiprocessed_worker_using_function
    )

    init_options = InitializerOptions(
        code=cli_args.code,
        module=cli_args.module,
        args=cli_args.args,
        init_codes=cli_args.init,
        before_codes=cli_args.before,
        after_codes=cli_args.after,
        row_len=reader.row_len,
        fieldnames=reader.fieldnames,
    )

    with get_pool(cli_args.processes, init_options) as pool:
        # NOTE: we keep track of rows being worked on from the main process
        # to avoid serializing them back with worker result.
        worked_rows = {}

        def payloads():
            for t in reader.enumerate():
                worked_rows[t[0]] = t[1]
                yield t

        mapper = pool.imap if not cli_args.unordered else pool.imap_unordered

        for i, result in mapper(worker, payloads(), chunksize=cli_args.chunk_size):
            row = worked_rows.pop(i)
            yield i, row, result


def map_action(cli_args, output_file):
    with Enricher(
        cli_args.file,
        output_file,
        add=[cli_args.new_column],
        delimiter=cli_args.delimiter,
    ) as enricher:
        for _, row, result in mp_iteration(cli_args, enricher):
            enricher.writerow(row, [serialize(result)])


def filter_action(cli_args, output_file):
    with Enricher(cli_args.file, output_file, delimiter=cli_args.delimiter) as enricher:
        for _, row, result in mp_iteration(cli_args, enricher):
            if result:
                enricher.writerow(row)


def reverse_action(cli_args, output_file):
    with Enricher(
        cli_args.file, output_file, delimiter=cli_args.delimiter, reverse=True
    ) as enricher:
        for row in enricher:
            enricher.writerow(row)
