'''
Get info on and manipulate jobs.
'''

import sys
import argparse
from ..client import Client
from ..common import configure_logging

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("jobs", nargs="*")
parser.add_argument(
    "--cleanup",
    action="store_true",
    default=False)


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


def run(argv=sys.argv[1:]):
    args = parser.parse_args(argv)
    configure_logging(args)

    client = Client.from_args(args)
    summary = client.job_summary(job_names=args.jobs if args.jobs else None)
    print(summary)
