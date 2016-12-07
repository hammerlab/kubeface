import logging
import subprocess


def check_call(*args, **kwargs):
    logging.info("Running: %s %s" % (args, kwargs))
    subprocess.check_call(*args, **kwargs)