import os
import shutil
import tempfile
import time
import logging

import kubeface
import kubeface.bucket_storage


logging.basicConfig(level=logging.DEBUG)

KEEP_FILES = os.environ.get("KUBEFACE_TEST_KEEP_FILES")


def wipe_bucket(bucket_url):
    objects = kubeface.bucket_storage.list_contents(bucket_url)
    for obj in objects:
        kubeface.bucket_storage.delete(bucket_url + "/" + obj)


def check_empty(bucket_url):
    assert not kubeface.bucket_storage.list_contents(bucket_url)


def with_bucket_storage(function):
    bucket = os.environ.get("KUBEFACE_BUCKET")
    if not bucket:
        logging.fatal("No bucket defined")

    def test_function():
        # check_empty("gs://" + bucket)
        wipe_bucket("gs://" + bucket)
        function("gs://" + bucket)
        wipe_bucket("gs://" + bucket)
    return test_function


def with_local_storage(function):
    def test_function():
        tempdir = tempfile.mkdtemp(dir='/tmp')
        function(tempdir)
        if not KEEP_FILES:
            shutil.rmtree(tempdir)
    return test_function


def with_local_and_bucket_storage(function):
    bucket = os.environ.get("KUBEFACE_BUCKET")
    if not bucket:
        logging.warning(
            "Set KUBEFACE_BUCKET to run test: %s" % str(function))
        return with_local_storage(function)

    def test_function():
        with_local_storage(function)()
        with_bucket_storage(function)()
    return test_function
