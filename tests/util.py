import os
import shutil
import tempfile
import logging


def with_bucket(function):
    bucket = os.environ.get("KUBEFACE_BUCKET")
    if bucket:
        def test_function():
            tempdir = None
            try:
                tempdir = tempfile.mkdtemp()
                function(tempdir)
            finally:
                if tempdir:
                    shutil.rmtree(tempdir)
            function("gs://" + bucket)
    else:
        def test_function():
            tempdir = None
            try:
                tempdir = tempfile.mkdtemp()
                function(tempdir)
            finally:
                if tempdir:
                    shutil.rmtree(tempdir)
            logging.warning(
                "Set KUBEFACE_BUCKET to run test: %s" % str(function))
    return test_function
