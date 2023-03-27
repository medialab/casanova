import sys
import math
import random
from os.path import join
from urllib.parse import urlsplit, urljoin
from multiprocessing import Pool as MultiProcessPool

from casanova import Enricher, CSVSerializer, RowWrapper, Headers


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


def get_pool(code, fieldnames, row_len, n=1):
    initargs = (code, fieldnames, row_len)

    if n < 2:
        multiprocessed_initializer(*initargs)
        return SingleProcessPool()

    return MultiProcessPool(
        n, initializer=multiprocessed_initializer, initargs=initargs
    )


serialize = CSVSerializer()

CODE = None
LOCAL_CONTEXT = {
    "join": join,
    "math": math,
    "random": random,
    "urljoin": urljoin,
    "urlsplit": urlsplit,
    "fieldnames": None,
    "headers": None,
    "index": 0,
    "row": None,
}


def multiprocessed_initializer(code, fieldnames, row_len):
    global CODE

    CODE = code

    if fieldnames is not None:
        LOCAL_CONTEXT["fieldnames"] = fieldnames
        LOCAL_CONTEXT["headers"] = Headers(fieldnames)
        headers = LOCAL_CONTEXT["headers"]
    else:
        headers = Headers(range(row_len))

    LOCAL_CONTEXT["row"] = RowWrapper(headers, None)


def multiprocessed_worker(payload):
    global LOCAL_CONTEXT

    i, row = payload
    LOCAL_CONTEXT["index"] = i
    LOCAL_CONTEXT["row"]._replace(row)

    return (i, eval(CODE, None, LOCAL_CONTEXT))


# TODO: -X/--exec, filter, reducer, reverse
def mp_iteration(cli_args, enricher):
    worker = WorkerWrapper(multiprocessed_worker)

    with get_pool(
        cli_args.code,
        enricher.fieldnames,
        enricher.row_len,
        cli_args.processes,
    ) as pool:
        # NOTE: we keep track of rows being worked on from the main process
        # to avoid serializing them back with worker result.
        worked_rows = {}

        def payloads():
            for t in enricher.enumerate():
                worked_rows[t[0]] = t[1]
                yield t

        for i, result in pool.imap(worker, payloads(), chunksize=cli_args.chunk_size):
            row = worked_rows.pop(i)
            yield i, row, result


def map_action(cli_args, output_file):
    with Enricher(cli_args.file, output_file, add=[cli_args.new_column]) as enricher:
        for i, row, result in mp_iteration(cli_args, enricher):
            enricher.writerow(row, [serialize(result)])
