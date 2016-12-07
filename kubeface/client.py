import logging
import subprocess
import hashlib
import getpass
import socket
import time
from datetime import datetime
from six import BytesIO

from .serialization import loads, dumps
from . import storage


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
            max_simultaneous_tasks=10,
            storage_prefix="gs://kubeface",
            run_locally=False):

        self.bucket_client = BucketClient(bucket)
        self.image = image
        self.image_pull_policy = image_pull_policy
        self.cluster = cluster
        self.python_path = python_path
        self.max_simultaneous_tasks = max_simultaneous_tasks
        self.storage_prefix = storage_prefix
        self.run_locally = run_locally

    def storage_path(self, filename):
        return self.storage_prefix + "/" + filename

    def launch_container(self, task_name):
        if run_locally:
            





    def run_job(self, tasks):
        job_name = make_job_name()
        for task in tasks:
            serialized = dumps(task)
            task_name = "task-%s-%s" % (
                job_name,
                hashlib.sha1(serialized).hexdigest())
            logging.debug("Uploading: %s" % task_name)
            storage.put(
                storage_path(task_name),
                BytesIO(serialized))





#--restart=OnFailure


