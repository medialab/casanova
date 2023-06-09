from typing import Optional, List

import re
import sys
import gzip
import json
import math
import random
import statistics
from itertools import islice
from types import GeneratorType
from os.path import join
from urllib.parse import urlsplit, urljoin
from multiprocessing import Pool as MultiProcessPool
from dataclasses import dataclass
from collections import Counter, defaultdict, deque, OrderedDict
from collections.abc import Mapping, Iterable

from casanova import (
    Reader,
    Enricher,
    CSVSerializer,
    RowWrapper,
    Headers,
    Writer,
    InferringWriter,
)
from casanova.utils import import_target, flatmap


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
    base_dir: Optional[str] = None


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


def get_csv_serializer(cli_args):
    return CSVSerializer(
        plural_separator=cli_args.plural_separator,
        none_value=cli_args.none_value,
        true_value=cli_args.true_value,
        false_value=cli_args.false_value,
    )


def get_inferring_writer(output_file, cli_args):
    return InferringWriter(
        output_file,
        fieldnames=cli_args.fieldnames,
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
EVALUATION_CONTEXT = {}
ROW = None
BASE_DIR = None


def read(path, encoding: str = "utf-8") -> Optional[str]:
    global BASE_DIR

    if BASE_DIR is not None:
        path = join(BASE_DIR, path)

    if path.endswith(".gz"):
        try:
            with gzip.open(path, encoding=encoding, mode="rt") as f:
                return f.read()
        except FileNotFoundError:
            return None

    try:
        with open(path, encoding="utf-8", mode="r") as f:
            return f.read()
    except FileNotFoundError:
        return None


EVALUATION_CONTEXT_LIB = {
    # lib
    "join": join,
    "math": math,
    "mean": statistics.mean,
    "median": statistics.median,
    "random": random,
    "re": re,
    "read": read,
    "urljoin": urljoin,
    "urlsplit": urlsplit,
    # classes
    "Counter": Counter,
    "defaultdict": defaultdict,
    "deque": deque,
}


def initialize_evaluation_context():
    global EVALUATION_CONTEXT

    EVALUATION_CONTEXT = {
        **EVALUATION_CONTEXT_LIB,
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
    global BASE_DIR

    # Reset in case of multiple execution from same process
    CODE = None
    FUNCTION = None
    ARGS = None
    SELECTION = None
    BEFORE_CODES = []
    AFTER_CODES = []
    ROW = None
    BASE_DIR = options.base_dir
    initialize_evaluation_context()

    if options.module:
        FUNCTION = import_target(options.code)
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


# TODO: go to minet for progress bar and rich?
# TODO: write proper cli documentation
def mp_iteration(cli_args, reader: Reader):
    worker = (
        multiprocessed_worker_using_eval
        if not cli_args.module
        else multiprocessed_worker_using_function
    )

    if cli_args.processes > 1:
        worker = WorkerWrapper(worker)

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
        base_dir=cli_args.base_dir,
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
    serialize = get_csv_serializer(cli_args)

    with Enricher(
        cli_args.file,
        output_file,
        add=[cli_args.new_column],
        delimiter=cli_args.delimiter,
    ) as enricher:
        for _, row, result in mp_iteration(cli_args, enricher):
            enricher.writerow(row, [serialize(result)])


def flatmap_action(cli_args, output_file):
    serialize = get_csv_serializer(cli_args)

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
            if cli_args.invert_match:
                result = not result

            if result:
                enricher.writerow(row)


def map_reduce_action(cli_args, output_file):
    acc_fn = None

    if cli_args.module:
        acc_fn = import_target(cli_args.accumulator)

    with Reader(
        cli_args.file,
        delimiter=cli_args.delimiter,
    ) as enricher:
        acc_context = EVALUATION_CONTEXT_LIB.copy()

        acc = None
        initialized = False

        if cli_args.init_value is not None:
            initialized = True
            acc = eval(cli_args.init_value, acc_context, None)

        acc_context["acc"] = acc

        for _, row, result in mp_iteration(cli_args, enricher):
            if not initialized:
                acc_context["acc"] = result
                initialized = True
                continue

            if acc_fn is None:
                acc_context["current"] = result
                acc_context["acc"] = eval(cli_args.accumulator, acc_context, None)
            else:
                acc_context["acc"] = acc_fn(acc_context["acc"], result)

        final_result = acc_context["acc"]

        if cli_args.json:
            json.dump(
                final_result,
                output_file,
                indent=2 if cli_args.pretty else None,
                ensure_ascii=False,
            )
            print(file=output_file)
        elif cli_args.csv:
            writer = get_inferring_writer(output_file, cli_args)
            writer.writerow(final_result)
        else:
            print(final_result, file=output_file)


class GroupWrapper:
    __slots__ = ("__name", "__rows", "__wrapper")

    def __init__(self, fieldnames):
        self.__wrapper = RowWrapper(Headers(fieldnames), range(len(fieldnames)))

    def _replace(self, name, rows):
        self.__name = name
        self.__rows = rows

    @property
    def name(self):
        return self.__name

    def __len__(self):
        return len(self.__rows)

    def __iter__(self):
        for row in self.__rows:
            self.__wrapper._replace(row)
            yield self.__wrapper


def groupby_action(cli_args, output_file):
    agg_fn = None

    if cli_args.module:
        agg_fn = import_target(cli_args.aggregator)

    with Reader(
        cli_args.file,
        delimiter=cli_args.delimiter,
    ) as enricher:
        # NOTE: using an ordered dict to guarantee stability for all python versions
        groups = OrderedDict()

        # Grouping
        for _, row, result in mp_iteration(cli_args, enricher):
            l = groups.get(result)

            if l is None:
                l = [row]
                groups[result] = l
            else:
                l.append(row)

        # Aggregating
        agg_context = EVALUATION_CONTEXT_LIB.copy()
        header_emitted = False

        writer = Writer(output_file)
        fieldnames = ["group"]
        mapping_fieldnames = None
        serializer = get_csv_serializer(cli_args)

        if cli_args.fieldnames is not None:
            mapping_fieldnames = cli_args.fieldnames
            fieldnames += cli_args.fieldnames
            header_emitted = True
            writer.writerow(fieldnames)

        group_wrapper = GroupWrapper(enricher.fieldnames)

        for name, rows in groups.items():
            group_wrapper._replace(name, rows)

            if agg_fn is not None:
                result = agg_fn(group_wrapper)
            else:
                agg_context["group"] = group_wrapper
                result = eval(cli_args.aggregator, agg_context, None)

            name = serializer(name)

            if isinstance(result, Mapping):
                if not header_emitted:
                    mapping_fieldnames = list(result.keys())
                    fieldnames += mapping_fieldnames
                    writer.writerow(fieldnames)
                    header_emitted = True

                writer.writerow(
                    [name] + serializer.serialize_dict_row(result, mapping_fieldnames)
                )
            elif isinstance(result, Iterable) and not isinstance(result, (bytes, str)):
                if not header_emitted:
                    fieldnames += ["col%i" % i for i in range(1, len(result) + 1)]
                    writer.writerow(fieldnames)
                    header_emitted = True

                writer.writerow([name] + serializer.serialize_row(result))
            else:
                if not header_emitted:
                    writer.writerow(fieldnames + ["value"])
                    header_emitted = True
                writer.writerow([name, serializer(result)])


def reverse_action(cli_args, output_file):
    with Enricher(
        cli_args.file, output_file, delimiter=cli_args.delimiter, reverse=True
    ) as enricher:
        it = enricher

        if cli_args.lines is not None:
            it = islice(enricher, cli_args.lines)

        for row in it:
            enricher.writerow(row)
