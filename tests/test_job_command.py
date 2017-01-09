import math
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


def run_job_command(bucket, argv):
    result = subprocess.check_output(
        ["kubeface-job", "--storage-prefix", bucket] + argv).decode()
    print(result)
    return result


def find_line_with(needle, haystack, nth=0):
    result = [x for x in haystack.split("\n") if needle in x][nth]
    print("Found line: %s" % result)
    return result


@util.with_local_storage
def test_job_command(bucket):
    c = client_from_commandline_args([
        "--poll-seconds", "1.1",
        "--backend", "local-process",
        "--storage-prefix", bucket,
    ])

    mapper = c.map(math.exp, range(10), cache_key='FOOBARBAZ')
    testing.assert_equal(next(mapper), 1)
    assert 'FOOBARBAZ' in run_job_command(bucket, [])
    assert 'active' in (
        find_line_with(
            "FOOBARBAZ",
            run_job_command(bucket, ["--include-done"]),
            nth=1))
    list(mapper)
    assert 'FOOBARBAZ' not in run_job_command(bucket, [])
