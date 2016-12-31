import numpy
import argparse
import subprocess
from numpy import testing

from kubeface import (
    client,
    common)

from . import util

common.configure_logging(verbose=True)


def client_from_commandline_args(argv):
    parser = argparse.ArgumentParser()
    client.Client.add_args(parser)
    args = parser.parse_args(argv)
    return client.Client.from_args(args)


def exercise_client(c, low=1, high=10):
    # Using division gives us an easy way to test handling of tasks
    # that throw division (by making low < 0) so it throws ZeroDivisionError
    testing.assert_equal(
        list(c.map(lambda x: 2.0 / x, range(low, high))),
        2.0 / numpy.arange(low, high))


def run_job_command(argv):
    return subprocess.check_output(["kubeface-job"] + argv)


@util.with_local_storage
def test_job_command(bucket):
    c = client_from_commandline_args([
        "--poll-seconds", "1.1",
        "--backend", "local-process",
        "--storage-prefix", bucket,
    ])

    mapper = c.map(lambda x: x + 5, range(10))
    print(run_job_command([]))
    print(run_job_command(["--include-done"]))

    '''
    exercise_client(c, high=2)
    testing.assert_equal(len(c.job_summary(include_done=False)), 0)
    testing.assert_equal(len(c.job_summary(include_done=True)), 2)

    mapper = c.map(lambda x: x + 5, range(10))
    testing.assert_equal(next(mapper), 5)
    testing.assert_equal(len(c.job_summary(include_done=False)), 1)
    testing.assert_equal(len(c.job_summary(include_done=True)), 3)
    testing.assert_equal(list(mapper), numpy.arange(1, 10) + 5)
    testing.assert_equal(len(c.job_summary(include_done=False)), 0)
    testing.assert_equal(len(c.job_summary(include_done=True)), 3)

    c.cleanup()
    testing.assert_equal(len(c.job_summary()), 0)
    '''
