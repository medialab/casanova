from typing import Optional

import os
import sys
import shlex
import platform
import multiprocessing
from argparse import ArgumentParser, HelpFormatter, ArgumentTypeError
from functools import partial

from casanova.utils import ensure_open
from casanova.cli import map_action, filter_action, reverse_action


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


class SortingHelpFormatter(HelpFormatter):
    def add_arguments(self, actions) -> None:
        actions = sorted(
            actions, key=lambda a: tuple(s.lower() for s in a.option_strings)
        )
        return super().add_arguments(actions)


VALID_ARG_NAMES = {"index", "row", "headers", "fieldnames"}


class ArgsType:
    def __call__(self, string):
        args = []

        for s in string.split(","):
            arg_name = s.strip().lower()

            if arg_name not in VALID_ARG_NAMES:
                raise ArgumentTypeError(
                    "%s is not a valid arg name. Must be one of: %s"
                    % (s, VALID_ARG_NAMES)
                )

            args.append(arg_name)

        return args


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
]


def add_arguments(parser: ArgumentParser, arguments):
    for args, kwargs in arguments:
        parser.add_argument(*args, **kwargs)


add_common_arguments = partial(add_arguments, arguments=COMMON_ARGUMENTS)
add_mp_arguments = partial(add_arguments, arguments=MP_ARGUMENTS)


def main(arguments_override: Optional[str] = None):
    parser = ArgumentParser(
        "casanova",
        description="Casanova command line utilities such as mapping, filtering, reducing columns of a given CSV files.",
        formatter_class=SortingHelpFormatter,
    )
    add_common_arguments(parser)

    subparsers = parser.add_subparsers(dest="action", help="Command to execute.")

    map_parser = subparsers.add_parser("map", formatter_class=SortingHelpFormatter)
    add_common_arguments(map_parser)
    add_mp_arguments(map_parser)
    map_parser.add_argument(
        "new_column",
        help="Name of the new column to create & containing the result of the evaluated code.",
    )
    map_parser.add_argument(
        "code", help="Python code to evaluate for each row of the CSV file."
    )
    map_parser.add_argument(
        "file",
        help="CSV file to map. Can be gzip-compressed, and can also be a URL. Will consider `-` as stdin.",
    )

    filter_parser = subparsers.add_parser(
        "filter", formatter_class=SortingHelpFormatter
    )
    add_common_arguments(filter_parser)
    add_mp_arguments(filter_parser)
    filter_parser.add_argument(
        "code", help="Python code to evaluate for each row of the CSV file."
    )
    filter_parser.add_argument(
        "file",
        help="CSV file to filter. Can be gzip-compressed, and can also be a URL. Will consider `-` as stdin.",
    )

    reverse_parser = subparsers.add_parser(
        "reverse", formatter_class=SortingHelpFormatter
    )
    add_common_arguments(reverse_parser)
    reverse_parser.add_argument(
        "file",
        help="CSV file to read in reverse. Can be gzip-compressed, and can also be a URL. Will consider `-` as stdin.",
    )

    commands = {
        "map": (map_parser, map_action),
        "filter": (filter_parser, filter_action),
        "reverse": (reverse_parser, reverse_action),
    }

    cli_args = parser.parse_args(
        shlex.split(arguments_override) if arguments_override is not None else None
    )

    if cli_args.action is None:
        parser.print_help()
        sys.exit(0)

    _, action = commands[cli_args.action]

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


if __name__ == "__main__":
    multiprocessing.freeze_support()
    multiprocessing.set_start_method("spawn")

    try:
        main()
    except (KeyboardInterrupt, BrokenPipeError):
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        sys.exit(1)
