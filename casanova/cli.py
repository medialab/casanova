from typing import Optional, List

import re
import sys
import math
import random
from os.path import join
from urllib.parse import urlsplit, urljoin
from multiprocessing import Pool as MultiProcessPool
from dataclasses import dataclass

from casanova import Enricher, CSVSerializer, RowWrapper, Headers


@dataclass
class InitializerOptions:
    code: str
    row_len: int
    init_code: Optional[str] = None
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
    global ROW

    CODE = options.code

    if options.fieldnames is not None:
        LOCAL_CONTEXT["fieldnames"] = options.fieldnames
        LOCAL_CONTEXT["headers"] = Headers(options.fieldnames)
        headers = LOCAL_CONTEXT["headers"]
    else:
        headers = Headers(range(options.row_len))

    if options.init_code is not None:
        exec(options.init_code, None, LOCAL_CONTEXT)

    LOCAL_CONTEXT["row"] = RowWrapper(headers, None)
    ROW = LOCAL_CONTEXT["row"]


def multiprocessed_worker(payload):
    global LOCAL_CONTEXT

    i, row = payload
    LOCAL_CONTEXT["index"] = i
    ROW._replace(row)

    return (i, eval(CODE, None, LOCAL_CONTEXT))


# TODO: -X/--exec, filter, reducer, reverse, conditional rich-argparse, --plural-separator etc., -I could be given multiple times, -b also (think about reduce before)
def mp_iteration(cli_args, enricher):
    worker = WorkerWrapper(multiprocessed_worker)

    init_options = InitializerOptions(
        code=cli_args.code,
        init_code=cli_args.init,
        row_len=enricher.row_len,
        fieldnames=enricher.fieldnames,
    )

    with get_pool(cli_args.processes, init_options) as pool:
        # NOTE: we keep track of rows being worked on from the main process
        # to avoid serializing them back with worker result.
        worked_rows = {}

        def payloads():
            for t in enricher.enumerate():
                worked_rows[t[0]] = t[1]
                yield t

        mapper = pool.imap if not cli_args.unordered else pool.imap_unordered

        for i, result in mapper(worker, payloads(), chunksize=cli_args.chunk_size):
            row = worked_rows.pop(i)
            yield i, row, result


def map_action(cli_args, output_file):
    with Enricher(cli_args.file, output_file, add=[cli_args.new_column]) as enricher:
        for _, row, result in mp_iteration(cli_args, enricher):
            enricher.writerow(row, [serialize(result)])
