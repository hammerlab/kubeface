import numpy
from numpy import testing

from kubeface import (
    client,
    local_process_backend,
    local_process_docker_backend,
    worker_configuration,
    common)

from .util import with_bucket

common.configure_logging(verbose=True)


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
    branch = "master"
    branch = "experiments"
    worker_config = worker_configuration.WorkerConfiguration(
        kubeface_install_command=(
        "{pip} install https://github.com/hammerlab/kubeface/archive/%s.zip" % branch))
    backend = local_process_docker_backend.LocalProcessDockerBackend(
        worker_configuration=worker_config)
    c = client.Client(
        backend,
        poll_seconds=1.0,
        max_simultaneous_tasks=1,
        storage_prefix=bucket)
    exercise_client(c, num=3)


def exercise_client(c, num=10):
    testing.assert_equal(
        list(c.map(lambda x: x**2, range(num))),
        numpy.arange(num)**2)
