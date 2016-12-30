import numpy
import argparse
import tempfile
from numpy import testing

from kubeface import (
    client,
    local_process_backend,
    local_process_docker_backend,
    worker_configuration,
    common)

from .util import with_bucket

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

@with_bucket
def test_local_process_backend(bucket):
    backend = local_process_backend.LocalProcessBackend()
    c = client.Client(
        backend,
        poll_seconds=1.0,
        max_simultaneous_tasks=3,
        storage_prefix=bucket)
    exercise_client(c)


@with_bucket
def test_local_process_docker_backend(bucket):
    worker_config = worker_configuration.WorkerConfiguration(
        kubeface_install_command="{pip} install /kubeface-package")
    backend = local_process_docker_backend.LocalProcessDockerBackend(
        worker_configuration=worker_config)
    c = client.Client(
        backend,
        poll_seconds=1.0,
        max_simultaneous_tasks=1,
        storage_prefix=bucket)
    exercise_client(c, high=3)


@with_bucket
def test_job_summary(bucket):
    c = client_from_commandline_args([
        "--poll-seconds", "1.1",
        "--backend", "local-process",
        "--storage-prefix", bucket,
    ])

    exercise_client(c, high=5)
    testing.assert_equal(len(c.job_summary(include_done=False)), 0)
    testing.assert_equal(len(c.job_summary(include_done=True)), 1)

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
