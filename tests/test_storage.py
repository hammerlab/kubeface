import os
import time
from six import BytesIO
from numpy import testing
import logging


from kubeface import bucket_storage, storage


def with_bucket(function):
    bucket = os.environ.get("KUBEFACE_BUCKET")
    if bucket:
        def test_function():
            return function(bucket)
    else:
        def test_function():
            logging.warning(
                "Set KUBEFACE_BUCKET to run test: %s" % str(function))
    return test_function


def test_url_parse():
    testing.assert_equal(
        bucket_storage.split_bucket_and_name("gs://foo/bar"),
        ("foo", "bar"))

    testing.assert_equal(
        bucket_storage.split_bucket_and_name("gs://foo/bar/baz.txt"),
        ("foo", "bar/baz.txt"))


@with_bucket
def test_put_and_get_to_bucket(bucket):
    data = "ABCDe" * 1000
    data_handle = BytesIO(data.encode("UTF-8"))
    name = "gs://%s/kubeface-test-%s.txt" % (
        bucket, str(time.time()).replace(".", ""))
    storage.put(name, data_handle)
    result_handle = storage.get(name)
    testing.assert_equal(result_handle.read().decode("UTF-8"), data)
    storage.delete(name)


