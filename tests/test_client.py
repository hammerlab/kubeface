import numpy
import argparse

from numpy import testing

from kubeface import (
    client,
    local_process_backend,
    local_process_docker_backend,
    worker_configuration,
    serialization,
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


@util.with_local_and_bucket_storage
def test_local_process_backend(bucket):
    backend = local_process_backend.LocalProcessBackend()
    c = client.Client(
        backend,
        poll_seconds=1.0,
        max_simultaneous_tasks=3,
        storage=bucket)
    exercise_client(c)


@util.with_local_storage
def test_local_process_docker_backend(bucket):
    worker_config = worker_configuration.WorkerConfiguration(
        kubeface_install_command="{pip} install /kubeface-package")
    backend = local_process_docker_backend.LocalProcessDockerBackend(
        worker_configuration=worker_config)
    c = client.Client(
        backend,
        poll_seconds=1.0,
        max_simultaneous_tasks=1,
        storage=bucket)
    exercise_client(c, high=3)


@util.with_local_and_bucket_storage
def test_worker_exception_delayed(bucket):
    c = client_from_commandline_args([
        "--kubeface-poll-seconds", "1.1",
        "--kubeface-backend", "local-process",
        "--kubeface-storage", bucket,
        "--kubeface-wait-to-raise-task-exception",
    ])
    mapper = c.map(lambda x: 2 / (x - 2), range(10))
    testing.assert_equal(next(mapper), -1)
    testing.assert_equal(next(mapper), -2)
    testing.assert_equal(len(c.job_summary(include_done=False)), 1)
    testing.assert_equal(len(c.job_summary(include_done=True)), 1)
    testing.assert_raises(ZeroDivisionError, next, mapper)
    testing.assert_equal(len(c.job_summary(include_done=False)), 0)
    testing.assert_equal(len(c.job_summary(include_done=True)), 1)
    testing.assert_raises(StopIteration, next, mapper)
    testing.assert_equal(len(c.job_summary(include_done=False)), 0)
    testing.assert_equal(len(c.job_summary(include_done=True)), 1)


@util.with_local_and_bucket_storage
def test_worker_exception(bucket):
    c = client_from_commandline_args([
        "--kubeface-poll-seconds", "1.1",
        "--kubeface-backend", "local-process",
        "--kubeface-storage", bucket,
        "--kubeface-cache-key-prefix", "foo",
    ])
    mapper = c.map(lambda x: 2 / (x - 2), range(10))
    testing.assert_raises(ZeroDivisionError, next, mapper)

    # TODO: in the future we may want reruns to not re-use excpetions.
    # Here is a test for that functionality, which is currently not
    # implemented.
    # c = client_from_commandline_args([
    #     "--kubeface-poll-seconds", "1.1",
    #     "--kubeface-backend", "local-process",
    #     "--kubeface-storage", bucket,
    #     "--kubeface-cache-key-prefix", "foo",
    # ])
    # results = list(c.map(lambda x: 2 / (x - 200), range(10)))
    # print(results)  # should not raise


@util.with_local_and_bucket_storage
def test_job_summary(bucket):
    c = client_from_commandline_args([
        "--kubeface-poll-seconds", "1.1",
        "--kubeface-backend", "local-process",
        "--kubeface-storage", bucket,
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


def test_invalid_client():
    with testing.assert_raises(ValueError):
        client_from_commandline_args([
            "--kubeface-poll-seconds", "1.1",
            "--kubeface-backend", "kubernetes",
            "--kubeface-storage", "/tmp",
        ])


@util.with_local_and_bucket_storage
def test_remote_object(bucket):
    c = client_from_commandline_args([
        "--kubeface-poll-seconds", "1.1",
        "--kubeface-backend", "local-process",
        "--kubeface-storage", bucket,
    ])
    data = numpy.arange(10000)**2
    serialized_data = serialization.dumps(data)
    testing.assert_equal(serialization.loads(serialized_data), data)

    remote = c.remote_object(data)
    serialized_remote = serialization.dumps(remote)
    assert len(serialized_remote) < len(serialized_data) / 10
    testing.assert_equal(serialization.loads(serialized_remote).value, data)


@util.with_local_and_bucket_storage
def test_pickle_client(bucket):
    c = client_from_commandline_args([
        "--kubeface-poll-seconds", "1.1",
        "--kubeface-backend", "local-process",
        "--kubeface-storage", bucket,
    ])
    testing.assert_equal(
        c.cache_key_prefix,
        serialization.loads(serialization.dumps(c)).cache_key_prefix)


@util.with_local_and_bucket_storage
def test_return_remote_object(bucket):
    c = client_from_commandline_args([
        "--kubeface-poll-seconds", "1.1",
        "--kubeface-backend", "local-process",
        "--kubeface-storage", bucket,
    ])
    mapper = c.map(lambda x: c.remote_object(x**2), range(10))
    obj = next(mapper)
    testing.assert_equal(obj.written, True)
    testing.assert_equal(obj.loaded, False)
    testing.assert_equal(obj.value, 0)
    testing.assert_equal(obj.loaded, True)
    testing.assert_equal(obj.value, 0)

    obj = next(mapper)
    testing.assert_equal(obj.written, True)
    testing.assert_equal(obj.loaded, False)
    testing.assert_equal(obj.value, 1)
    testing.assert_equal(obj.loaded, True)
    testing.assert_equal(obj.value, 1)

    obj = next(mapper)
    testing.assert_equal(obj.written, True)
    testing.assert_equal(obj.loaded, False)
    testing.assert_equal(obj.value, 4)
    testing.assert_equal(obj.loaded, True)
    testing.assert_equal(obj.value, 4)
