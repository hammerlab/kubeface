'''
Run a task. Used internally, not meant to be called by a user.
'''

import sys
import argparse
import logging
import subprocess
import numpy
import csv
from functools import partial

from ..client import Client
from ..common import configure_logging

EVAL_ENVIRONMENT = {
    "numpy": numpy,
}

parser = argparse.ArgumentParser(description=__doc__)
command_group = parser.add_mutually_exclusive_group()
command_group.add_argument("--shell-command")
command_group.add_argument("--expression")

parser.add_argument("--generator-expression", required=True)

parser.add_argument("--out-csv")

Client.add_args(parser)


parser.add_argument(
    "--quiet",
    action="store_true",
    default=False,
    help="")

parser.add_argument(
    "--verbose",
    action="store_true",
    default=False,
    help="")


def shell_command_task(shell_command, value):
    interpolated = shell_command.format(value)
    logging.info("Running shell command: %s" % interpolated)
    result = subprocess.check_output(interpolated, shell=True)
    return (value, result)


def expression_task(expression, value):
    return (value, eval(expression, EVAL_ENVIRONMENT, {"value": value}))


def generator_from_expression(expression):
    for value in eval(expression, EVAL_ENVIRONMENT):
        yield value


def run(argv=sys.argv[1:]):
    args = parser.parse_args(argv)
    configure_logging(args)

    client = Client.from_args(args)

    if args.shell_command:
        task_function = partial(shell_command_task, args.shell_command)
    elif args.expression:
        task_function = partial(expression_task, args.expression)
    else:
        parser.error("Must specify --shell-command or --expression")

    if args.generator_expression:
        generator = generator_from_expression(
            args.generator_expression)
    else:
        parser.error("Must specify --generator")

    results = client.map(task_function, generator)

    if args.out_csv:
        writer = csv.writer(open(args.out_csv, "w"))
    else:
        writer = csv.writer(sys.stdout)

    writer.writerow(["Value", "Result"])

    for (value, return_value) in results:
        writer.writerow([str(value), str(return_value)])

    logging.info("Wrote: %s" % (args.out_csv if args.out_csv else "(stdout)"))
