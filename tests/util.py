import os
import shutil
import tempfile
import logging

logging.basicConfig(level=logging.DEBUG)

KEEP_FILES = os.environ.get("KUBEFACE_TEST_KEEP_FILES")


def with_bucket_storage(function):
    bucket = os.environ.get("KUBEFACE_BUCKET")
    if not bucket:
        logging.fatal("No bucket defined")

    def test_function():
        function("gs://" + bucket)
    return test_function


def with_local_storage(function):
    def test_function():
        tempdir = None
        try:
            tempdir = tempfile.mkdtemp(dir='/tmp')
            function(tempdir)
        finally:
            if tempdir and not KEEP_FILES:
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
