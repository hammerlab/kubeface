from math import sqrt

import numpy
from numpy import testing

from joblib import Parallel, parallel_backend

import kubeface.joblib_backend

kubeface.joblib_backend.configure()

kubeface_args = {
    "python": "venv-py3/bin/python",
    "bucket": "gs://kubeface-data/",
    "container": {
        "image": "hammerlab/mhcflurry-misc:dev1",
        "imagePullPolicy": "Always",
    },
}


def test_basic():
    result = Parallel(n_jobs=10)((sqrt, [i**2], {}) for i in range(10))
    testing.assert_equal(result, numpy.arange(10))

    with parallel_backend('kubeface', **kubeface_args):
        result = Parallel(n_jobs=10)((sqrt, [i], {}) for i in range(10))
        testing.assert_equal(result, numpy.arange(10))
        print(result)

