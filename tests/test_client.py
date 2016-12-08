import numpy
from numpy import testing

from kubeface import client, local_process_backend

from .util import with_bucket


@with_bucket
def test_local_process_backend(bucket):
    backend = local_process_backend.LocalProcessBackend()
    c = client.Client(
        backend,
        max_simultaneous_tasks=3,
        storage_prefix=bucket)
    exercise_client(c)


def exercise_client(c):
    testing.assert_equal(
        list(c.map(lambda x: x**2, range(10))),
        numpy.arange(10)**2)
