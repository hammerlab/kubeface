import logging
import subprocess
import sys


def check_call(*args, **kwargs):
    logging.info("Running: %s %s" % (args, kwargs))
    subprocess.check_call(*args, **kwargs)


def configure_logging(args=None, verbose=False):
    if verbose or (args is not None and args.verbose):
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(
        format="%(asctime)s.%(msecs)d %(levelname)s %(module)s - %(funcName)s:"
        " %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
        level=level)
