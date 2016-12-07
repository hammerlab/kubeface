import logging
import subprocess
import hashlib
import getpass
import socket
import time
from datetime import datetime
from six import StringIO

from .serialization import loads, dumps
from .bucket_client import BucketClient


def make_job_name():
    job_name = "%s-%s-%s" % (
        socket.gethostbyname(),
        getpass.getuser(),
        datetime.strftime(datetime.now(), "%Y-%m-%d-%H:%M:%S"),
        hashlib.sha1(str(time.time()).encode()).hexdigest()[:8])
    return job_name


class Client(object):
    def __init__(
            self,
            image,
            bucket,
            image_pull_policy="Always",
            cluster=None,
            python_path='/usr/bin/env python',
            available_parallelism=10,
            run_locally=False):

        self.bucket_client = BucketClient(bucket)
        self.image = image
        self.image_pull_policy = image_pull_policy
        self.cluster = cluster
        self.python_path = python_path
        self.available_parallelism = available_parallelism

    def launch_container(self, task_name):
        if run_locally:
            




    def submit_job(self, tasks):
        job_name = make_job_name()
        for task in tasks:
            serialized = dumps(task)
            task_name = "task-%s-%s" % (
                job_name,
                hashlib.sha1(serialized).hexdigest())
            logging.debug("Uploading: %s" % task_name)
            self.bucket_client.upload(task_name, StringIO(serialized))





#--restart=OnFailure


