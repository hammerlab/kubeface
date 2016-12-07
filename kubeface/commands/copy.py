'''
Copy files, including support for google storage buckets.
'''

import sys
import argparse
import logging

from .. import storage

parser = argparse.ArgumentParser(description=__doc__)

parser.add_argument("source")
parser.add_argument("destination")

parser.add_argument(
    "--no-error",
    action="store_true",
    default=False,
    help="")

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

    logging.info("Reading: %s" % args.source)
    input_handle = storage.get(args.source)

    if args.destination == "-":
        print(input_handle.read())
    else:
        logging.info("Writing: %s" % args.destination)
    storage.put(args.destination, input_handle)

    logging.info("Completed.")
