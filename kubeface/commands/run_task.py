'''
Run a task. Used internally, not meant to be called by a user.
'''

import sys
import argparse
import logging
import tempfile
import math
import signal
import traceback
import os

from .. import storage, serialization
from ..common import configure_logging

parser = argparse.ArgumentParser(description=__doc__)

parser.add_argument("input_path")
parser.add_argument("result_path")

parser.add_argument(
    "--delete-input",
    action="store_true",
    default=False,
    help="Delete input file on success.")

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

    # On sigusr1 print stack trace
    print("To show stack trace, run:\nkill -s USR1 %d" % os.getpid())
    signal.signal(signal.SIGUSR1, lambda sig, frame: traceback.print_stack())

    configure_logging(args)

    logging.info("Reading: %s" % args.input_path)
    input_handle = storage.get(args.input_path)
    task = serialization.load(input_handle)

    logging.info("Deserialized task: %s" % task)
    logging.info("Running task.")
    result = task.run(input_size=input_handle.tell())
    logging.info("Done running task.")

    result_path = args.result_path.format(
        result_type=result.result_type,
        result_time=int(math.ceil(result.end_time)))

    with tempfile.TemporaryFile(
            prefix="kubeface-run-task-result-", suffix=".pkl") as fd:
        logging.info("Serializing result.")
        serialization.dump(result, fd)
        logging.info("Serialized result to %d bytes." % fd.tell())
        fd.seek(0)
        logging.info("Writing: %s" % result_path)
        storage.put(result_path, fd)

    if args.delete_input:
        logging.info("Deleting: %s" % args.input_path)
        storage.delete(args.input_path)

    logging.info("Done.")
