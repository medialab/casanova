import os
import sys
import platform
import multiprocessing
from argparse import ArgumentParser

from casanova.utils import ensure_open
from casanova.cli import map_action


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


COMMON_ARGUMENTS = [
    (
        ("-d", "--delimiter"),
        {"help": 'CSV delimiter to use. Defaults to ",".', "default": ","},
    ),
    (
        ("-o", "--output"),
        {"help": "Path to the output file. Will default to stdout."},
    ),
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
]


def add_common_arguments(parser: ArgumentParser):
    for args, kwargs in COMMON_ARGUMENTS:
        parser.add_argument(*args, **kwargs)


def main():
    multiprocessing.freeze_support()
    multiprocessing.set_start_method("spawn")

    parser = ArgumentParser(
        "casanova",
        description="Casanova command line utilities such as mapping, filtering, reducing columns of a given CSV files.",
    )
    add_common_arguments(parser)

    subparsers = parser.add_subparsers(dest="action", help="Command to execute.")

    map_parser = subparsers.add_parser("map")
    add_common_arguments(map_parser)
    map_parser.add_argument(
        "new_column",
        help="Name of the new column to create & containing the result of the evaluated code.",
    )
    map_parser.add_argument(
        "code", help="Python code to evaluate for each row of the CSV file."
    )
    map_parser.add_argument(
        "file",
        help="CSV file to process. Can be compressed, and can also be a URL. If not given, the command will fallback to read the file from stdin.",
        default=sys.stdin,
        nargs="?",
    )

    commands = {"map": (map_parser, map_action)}

    args = parser.parse_args()

    if args.action is None:
        parser.print_help()
        sys.exit(0)

    _, action = commands[args.action]

    if args.output is None:
        action(args, acquire_cross_platform_stdout())
    else:
        with ensure_open(args.output, "w", encoding="utf-8", newline="") as output_file:
            action(args, output_file)


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, BrokenPipeError):
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        sys.exit(1)
