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


def human_readable_memory_size(num, suffix='B'):
    # From: http://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)


def truncate(s, max_length):
    if len(s) < max_length:
        return s
    return s[:max_length] + "..."
