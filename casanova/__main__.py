from typing import Optional

import os
import sys
import shlex
import shutil
import platform
import multiprocessing
from argparse import ArgumentParser, HelpFormatter, ArgumentTypeError
from functools import partial

from casanova.defaults import set_defaults
from casanova.utils import ensure_open, LT_PY311
from casanova.cli import (
    map_action,
    flatmap_action,
    filter_action,
    map_reduce_action,
    groupby_action,
    reverse_action,
)


def acquire_cross_platform_stdout():
    # As per #254: stdout need to be wrapped so that windows get a correct csv
    # stream output
    if "windows" in platform.system().lower():
        return open(
            sys.__stdout__.fileno(),
            mode=sys.__stdout__.mode,
            buffering=1,
            encoding=sys.__stdout__.encoding,
            errors=sys.__stdout__.errors,
            newline="",
            closefd=False,
        )

    return sys.stdout


def print_err(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def die(*args):
    print_err(*args)
    sys.exit(1)


class SortingHelpFormatter(HelpFormatter):
    def add_arguments(self, actions) -> None:
        actions = sorted(
            actions, key=lambda a: tuple(s.lower() for s in a.option_strings)
        )
        return super().add_arguments(actions)


def custom_formatter(prog):
    terminal_size = shutil.get_terminal_size()
    return SortingHelpFormatter(prog, width=terminal_size.columns, max_help_position=32)


VALID_ARG_NAMES = {"index", "row", "headers", "fieldnames", "cell", "cells"}


class ArgsType:
    def __call__(self, string):
        args = []

        if not string.strip():
            return args

        for s in string.split(","):
            arg_name = s.strip().lower()

            if arg_name not in VALID_ARG_NAMES:
                raise ArgumentTypeError(
                    "%s is not a valid arg name. Must be one of: %s"
                    % (s, VALID_ARG_NAMES)
                )

            args.append(arg_name)

        return args


class SpliterType:
    def __init__(self, splitchar=","):
        self.splitchar = splitchar

    def __call__(self, string):
        return string.split(self.splitchar)


class PositiveIntegerType:
    def __call__(self, string):
        try:
            number = int(string)
        except ValueError:
            raise ArgumentTypeError("expecting a non-zero positive integer")

        if number < 1:
            raise ArgumentTypeError("expecting a non-zero positive integer")

        return number


COMMON_ARGUMENTS = [
    (
        ("-d", "--delimiter"),
        {"help": 'CSV delimiter to use. Defaults to ",".', "default": ","},
    ),
    (
        ("-o", "--output"),
        {
            "help": "Path to the output file. Will default to stdout and will consider `-` as stdout."
        },
    ),
]

MP_ARGUMENTS = [
    (
        ("-p", "--processes"),
        {
            "help": "Number of processes to use. Defaults to 1.",
            "default": 1,
            "type": int,
        },
    ),
    (
        (
            "-c",
            "--chunk-size",
        ),
        {
            "help": "Multiprocessing chunk size. Defaults to 1.",
            "default": 1,
            "type": int,
        },
    ),
    (
        ("-u", "--unordered"),
        {
            "help": "Whether you allow the result to be in arbitrary order when using multiple processes. Defaults to no.",
            "action": "store_true",
        },
    ),
    (
        ("-I", "--init"),
        {
            "help": "Code to execute once before starting to iterate over file. Useful to setup global variables used in evaluated code later. Can be given multiple times.",
            "action": "append",
            "default": [],
        },
    ),
    (
        ("-B", "--before"),
        {
            "help": "Code to execute before each evaluation of code for a row in the CSV file. Useful to update variables before returning something. Can be given multiple times.",
            "action": "append",
            "default": [],
        },
    ),
    (
        ("-A", "--after"),
        {
            "help": "Code to execute after each evaluation of code for a row in the CSV file. Useful to update variables after having returned something. Can be given multiple times.",
            "action": "append",
            "default": [],
        },
    ),
    (
        ("-m", "--module"),
        {
            "help": "If set, given code will be interpreted as a python module to import and a function name taking the current index and row.",
            "action": "store_true",
        },
    ),
    (
        ("-a", "--args"),
        {
            "help": 'List of arguments to pass to the function when using -m/--module. Defaults to "row".',
            "default": ["row"],
            "type": ArgsType(),
        },
    ),
    (
        ("-i", "--ignore-errors"),
        {
            "help": "If set, evaluation raising an error will be considered as returning None instead of raising.",
            "action": "store_true",
        },
    ),
    (
        ("-s", "--select"),
        {
            "help": 'Use to select columns. First selected column value will be forwared as "cell" and selected column values as "cells".'
        },
    ),
    (
        ("-b", "--base-dir"),
        {"help": 'Base directory to be used by the "read" function.'},
    ),
]

SERIALIZATION_ARGUMENTS = [
    (
        ("--plural-separator",),
        {
            "help": 'Character to use to join lists and sets together in a single cell. Defaults to "|". If you need to emit multiple rows instead, consider using flatmap.',
            "default": "|",
        },
    ),
    (
        ("--none-value",),
        {
            "help": "String used to serialize None values. Defaults to an empty string.",
            "default": "",
        },
    ),
    (
        ("--true-value",),
        {
            "help": 'String used to serialize True values. Defaults to "true".',
            "default": "true",
        },
    ),
    (
        ("--false-value",),
        {
            "help": 'String used to serialize False values. Defaults to "false".',
            "default": "false",
        },
    ),
]

FORMAT_ARGUMENTS = [
    (
        ("--json",),
        {"help": "Whether to format the output as json.", "action": "store_true"},
    ),
    (
        ("--pretty",),
        {
            "help": "Whether to prettify the output, e.g. indent the json file.",
            "action": "store_true",
        },
    ),
    (
        ("--csv",),
        {"help": "Whether to format the output as csv.", "action": "store_true"},
    ),
]


def add_arguments(parser: ArgumentParser, arguments):
    for args, kwargs in arguments:
        parser.add_argument(*args, **kwargs)


add_common_arguments = partial(add_arguments, arguments=COMMON_ARGUMENTS)
add_mp_arguments = partial(add_arguments, arguments=MP_ARGUMENTS)
add_serialization_arguments = partial(add_arguments, arguments=SERIALIZATION_ARGUMENTS)
add_format_arguments = partial(add_arguments, arguments=FORMAT_ARGUMENTS)


def build_commands():
    parser = ArgumentParser(
        "casanova",
        description="Casanova command line utilities such as mapping, filtering, reducing columns of a given CSV files.",
        formatter_class=custom_formatter,
    )
    add_common_arguments(parser)

    subparsers = parser.add_subparsers(dest="action", help="Command to execute.")

    map_parser = subparsers.add_parser("map", formatter_class=custom_formatter)
    add_common_arguments(map_parser)
    add_mp_arguments(map_parser)
    add_serialization_arguments(map_parser)
    map_parser.add_argument(
        "code", help="Python code to evaluate for each row of the CSV file."
    )
    map_parser.add_argument(
        "new_column",
        help="Name of the new column to create & containing the result of the evaluated code.",
    )
    map_parser.add_argument(
        "file",
        help="CSV file to map. Can be gzip-compressed, and can also be a URL. Will consider `-` as stdin.",
    )

    flatmap_parser = subparsers.add_parser("flatmap", formatter_class=custom_formatter)
    add_common_arguments(flatmap_parser)
    add_mp_arguments(flatmap_parser)
    add_serialization_arguments(flatmap_parser)
    flatmap_parser.add_argument(
        "code", help="Python code to evaluate for each row of the CSV file."
    )
    flatmap_parser.add_argument(
        "new_column",
        help="Name of the new column to create & containing the result of the evaluated code.",
    )
    flatmap_parser.add_argument(
        "file",
        help="CSV file to flatmap. Can be gzip-compressed, and can also be a URL. Will consider `-` as stdin.",
    )

    filter_parser = subparsers.add_parser("filter", formatter_class=custom_formatter)
    add_common_arguments(filter_parser)
    add_mp_arguments(filter_parser)
    filter_parser.add_argument(
        "code", help="Python code to evaluate for each row of the CSV file."
    )
    filter_parser.add_argument(
        "file",
        help="CSV file to filter. Can be gzip-compressed, and can also be a URL. Will consider `-` as stdin.",
    )
    filter_parser.add_argument(
        "-v",
        "--invert-match",
        help="Reverse the condition used to filter.",
        action="store_true",
    )

    map_reduce_parser = subparsers.add_parser(
        "map-reduce", formatter_class=custom_formatter
    )
    add_common_arguments(map_reduce_parser)
    add_mp_arguments(map_reduce_parser)
    add_serialization_arguments(map_reduce_parser)
    add_format_arguments(map_reduce_parser)
    map_reduce_parser.add_argument(
        "code", help="Python code to evaluate for each row of the CSV file."
    )
    map_reduce_parser.add_argument(
        "accumulator",
        help="Python code that will be evaluated to perform the accumulation towards a final value.",
    )
    map_reduce_parser.add_argument(
        "file",
        help="CSV file to map-reduce. Can be gzip-compressed, and can also be a URL. Will consider `-` as stdin.",
    )
    map_reduce_parser.add_argument(
        "-V",
        "--init-value",
        help="Python code to evaluate to initialize the accumulator's value. If not given, the initial value will be the first map result.",
    )
    map_reduce_parser.add_argument(
        "-f",
        "--fieldnames",
        help="Output CSV file fieldnames. Useful when emitting sequences without keys (e.g. lists, tuples etc.).",
        type=SpliterType(),
    )

    groupby_parser = subparsers.add_parser("groupby", formatter_class=custom_formatter)
    add_common_arguments(groupby_parser)
    add_mp_arguments(groupby_parser)
    add_serialization_arguments(groupby_parser)
    groupby_parser.add_argument(
        "code", help="Python code to evaluate to group each row of the CSV file."
    )
    groupby_parser.add_argument(
        "aggregator",
        help="Python code that will be evaluated to perform the aggregation of each yielded group of rows.",
    )
    groupby_parser.add_argument(
        "file",
        help="CSV file to group. Can be gzip-compressed, and can also be a URL. Will consider `-` as stdin.",
    )
    groupby_parser.add_argument(
        "-f",
        "--fieldnames",
        help="Output CSV file fieldnames. Useful when emitting sequences without keys (e.g. lists, tuples etc.).",
        type=SpliterType(),
    )

    reverse_parser = subparsers.add_parser("reverse", formatter_class=custom_formatter)
    add_common_arguments(reverse_parser)
    reverse_parser.add_argument(
        "file",
        help="CSV file to read in reverse. Can be gzip-compressed, and can also be a URL. Will consider `-` as stdin.",
    )
    reverse_parser.add_argument(
        "-n", "--lines", help="Number of lines to read.", type=PositiveIntegerType()
    )

    commands = {
        "map": (map_parser, map_action),
        "flatmap": (flatmap_parser, flatmap_action),
        "filter": (filter_parser, filter_action),
        "map-reduce": (map_reduce_parser, map_reduce_action),
        "groupby": (groupby_parser, groupby_action),
        "reverse": (reverse_parser, reverse_action),
    }

    return parser, commands


CASANOVA_PARSER, CASANOVA_COMMANDS = build_commands()


def run(arguments_override: Optional[str] = None):
    cli_args = CASANOVA_PARSER.parse_args(
        shlex.split(arguments_override) if arguments_override is not None else None
    )

    if cli_args.action is None:
        CASANOVA_PARSER.print_help()
        sys.exit(0)

    _, action = CASANOVA_COMMANDS[cli_args.action]

    # Validating
    args_flag = getattr(cli_args, "args", [])

    if "cell" in args_flag or "cells" in args_flag:
        if not getattr(cli_args, "select", None):
            die('Cannot use "cell" or "cells" in --args without providing -s/--select!')

    # Stdin fallback
    if getattr(cli_args, "file", None) == "-":
        cli_args.file = sys.stdin

    # Dealing with output stream
    if cli_args.output is None or cli_args.output == "-":
        action(cli_args, acquire_cross_platform_stdout())
    else:
        with ensure_open(
            cli_args.output, "w", encoding="utf-8", newline=""
        ) as output_file:
            action(cli_args, output_file)


def main():
    multiprocessing.freeze_support()
    multiprocessing.set_start_method("spawn")

    if LT_PY311:
        set_defaults(strip_null_bytes_on_read=True, strip_null_bytes_on_write=True)

    try:
        run()
    except (KeyboardInterrupt, BrokenPipeError):
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        sys.exit(1)


if __name__ == "__main__":
    main()
