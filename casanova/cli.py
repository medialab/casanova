from typing import Optional, List

import re
import sys
import math
import random
import statistics
from types import GeneratorType
from os.path import join
from urllib.parse import urlsplit, urljoin
from multiprocessing import Pool as MultiProcessPool
from dataclasses import dataclass
from collections import Counter, defaultdict, deque


from casanova import Reader, Enricher, CSVSerializer, RowWrapper, Headers
from casanova.utils import import_function, flatmap


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
    selected_indices: Optional[List[int]] = None


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


def get_serializer(cli_args):
    return CSVSerializer(
        plural_separator=cli_args.plural_separator,
        none_value=cli_args.none_value,
        true_value=cli_args.true_value,
        false_value=cli_args.false_value,
    )


# Global multiprocessing variables
CODE = None
FUNCTION = None
ARGS = None
SELECTION = None
BEFORE_CODES = []
AFTER_CODES = []
EVALUATION_CONTEXT = None
ROW = None


def initialize_evaluation_context():
    global EVALUATION_CONTEXT

    EVALUATION_CONTEXT = {
        # lib
        "join": join,
        "math": math,
        "mean": statistics.mean,
        "median": statistics.median,
        "random": random,
        "re": re,
        "urljoin": urljoin,
        "urlsplit": urlsplit,
        # classes
        "Counter": Counter,
        "defaultdict": defaultdict,
        "deque": deque,
        # state
        "fieldnames": None,
        "headers": None,
        "index": 0,
        "row": None,
        "cell": None,
        "cells": None,
    }


def multiprocessed_initializer(options: InitializerOptions):
    global CODE
    global FUNCTION
    global ARGS
    global BEFORE_CODES
    global AFTER_CODES
    global ROW
    global SELECTION

    # Reset in case of multiple execution from same process
    CODE = None
    FUNCTION = None
    ARGS = None
    SELECTION = None
    BEFORE_CODES = []
    AFTER_CODES = []
    ROW = None
    initialize_evaluation_context()

    if options.module:
        FUNCTION = import_function(options.code)
        ARGS = options.args
    else:
        CODE = options.code
        BEFORE_CODES = options.before_codes
        AFTER_CODES = options.after_codes

    if options.selected_indices is not None:
        SELECTION = options.selected_indices

    if options.fieldnames is not None:
        EVALUATION_CONTEXT["fieldnames"] = options.fieldnames
        EVALUATION_CONTEXT["headers"] = Headers(options.fieldnames)
        headers = EVALUATION_CONTEXT["headers"]
    else:
        headers = Headers(range(options.row_len))

    for init_code in options.init_codes:
        exec(init_code, None, EVALUATION_CONTEXT)

    EVALUATION_CONTEXT["row"] = RowWrapper(headers, None)
    ROW = EVALUATION_CONTEXT["row"]


def select(row):
    if SELECTION is None:
        return

    cells = [row[i] for i in SELECTION]
    EVALUATION_CONTEXT["cells"] = cells
    EVALUATION_CONTEXT["cell"] = cells[0]


def multiprocessed_worker_using_eval(payload):
    global EVALUATION_CONTEXT

    i, row = payload
    EVALUATION_CONTEXT["index"] = i
    ROW._replace(row)

    select(row)

    try:
        for before_code in BEFORE_CODES:
            exec(before_code, EVALUATION_CONTEXT, None)

        value = eval(CODE, EVALUATION_CONTEXT, None)

        for after_code in AFTER_CODES:
            exec(after_code, EVALUATION_CONTEXT, None)

        return None, i, value
    except Exception as e:
        return e, i, None


def collect_args(i, row):
    for arg_name in ARGS:
        if arg_name == "row":
            yield ROW
        elif arg_name == "index":
            yield i
        elif arg_name == "fieldnames":
            yield EVALUATION_CONTEXT["fieldnames"]
        elif arg_name == "headers":
            yield EVALUATION_CONTEXT["headers"]
        elif arg_name == "cell":
            # NOTE: we know SELECTION is relevant because it's validated by CLI
            yield row[SELECTION[0]]
        elif arg_name == "cells":
            # NOTE: we know SELECTION is relevant because it's validated by CLI
            for idx in SELECTION:
                yield row[idx]
        else:
            raise TypeError("unknown arg_name: %s" % arg_name)


def multiprocessed_worker_using_function(payload):
    i, row = payload
    ROW._replace(row)

    args = tuple(collect_args(i, row))

    try:
        value = FUNCTION(*args)

        # NOTE: consuming generators
        if isinstance(value, GeneratorType):
            value = list(value)

        return None, i, value
    except Exception as e:
        return e, i, None


# TODO: reduce, groupby? -> serialize lists/dicts or --raw or --json
# TODO: go to minet for progress bar and rich?
# TODO: write proper cli documentation
def mp_iteration(cli_args, reader: Reader):
    worker = WorkerWrapper(
        multiprocessed_worker_using_eval
        if not cli_args.module
        else multiprocessed_worker_using_function
    )

    selected_indices = None

    if cli_args.select:
        if reader.headers is not None:
            selected_indices = reader.headers.select(cli_args.select)
        else:
            selected_indices = Headers.select_no_headers(cli_args.select)

    init_options = InitializerOptions(
        code=cli_args.code,
        module=cli_args.module,
        args=cli_args.args,
        init_codes=cli_args.init,
        before_codes=cli_args.before,
        after_codes=cli_args.after,
        row_len=reader.row_len,
        fieldnames=reader.fieldnames,
        selected_indices=selected_indices,
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

        for exc, i, result in mapper(worker, payloads(), chunksize=cli_args.chunk_size):
            row = worked_rows.pop(i)

            if exc is not None:
                if cli_args.ignore_errors:
                    result = None
                else:
                    raise exc

            yield i, row, result


def map_action(cli_args, output_file):
    serialize = get_serializer(cli_args)

    with Enricher(
        cli_args.file,
        output_file,
        add=[cli_args.new_column],
        delimiter=cli_args.delimiter,
    ) as enricher:
        for _, row, result in mp_iteration(cli_args, enricher):
            enricher.writerow(row, [serialize(result)])


def flatmap_action(cli_args, output_file):
    serialize = get_serializer(cli_args)

    with Enricher(
        cli_args.file,
        output_file,
        add=[cli_args.new_column],
        delimiter=cli_args.delimiter,
    ) as enricher:
        for _, row, result in mp_iteration(cli_args, enricher):
            for value in flatmap(result):
                enricher.writerow(row, [serialize(value)])


def filter_action(cli_args, output_file):
    with Enricher(cli_args.file, output_file, delimiter=cli_args.delimiter) as enricher:
        for _, row, result in mp_iteration(cli_args, enricher):
            if result:
                enricher.writerow(row)


def reduce_action(cli_args, output_file):
    cli_args.init.insert(0, "acc = %s" % cli_args.acc)
    cli_args.code = "acc = (%s)" % cli_args.code

    with Reader(
        cli_args.file,
        delimiter=cli_args.delimiter,
    ) as enricher:
        for _, row, result in mp_iteration(cli_args, enricher):
            pass

        print(result, file=output_file)


def reverse_action(cli_args, output_file):
    with Enricher(
        cli_args.file, output_file, delimiter=cli_args.delimiter, reverse=True
    ) as enricher:
        for row in enricher:
            enricher.writerow(row)