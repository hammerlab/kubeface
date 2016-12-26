import subprocess
import logging

from kubeface import worker_payload

logging.basicConfig(level=logging.DEBUG)


def test_payload():
    args = worker_payload.payload_python_args()
    print(args)
    subprocess.check_call(args + ["--help"])
