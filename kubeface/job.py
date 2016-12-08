import logging
import hashlib
import time
import socket
import datetime
import getpass
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


class Job(object):
    def __init__(
            self,
            backend,
            tasks_iter,
            max_simultaneous_tasks,
            storage_prefix):

        self.backend = backend
        self.max_simultaneous_tasks = max_simultaneous_tasks
        self.storage_prefix = storage_prefix
        self.tasks_iter = tasks_iter

        self.name = make_job_name()
        self.task_name_prefix = "task-%s-input" % self.name
        self.submitted_tasks = []

    def storage_path(self, filename):
        return self.storage_prefix + "/" + filename

    def submit_one_task(self):
        try:
            task = next(self.tasks_iter)
        except StopIteration:
            return False

        serialized = dumps(task)
        task_hash = hashlib.sha1(serialized).hexdigest()
        task_input = "task-%s-input-%s" % (self.name, task_hash)
        task_output = "task-%s-output-%s" % (self.name, task_hash)
        logging.debug("Uploading: %s" % task_input)
        storage.put(
            self.storage_path(task_input),
            BytesIO(serialized))
        self.backend.submit_task(
            task_input,
            task_output)
        self.submitted_tasks.append(task_name)
        return True

    def get_completed_tasks(self):
        completed_tasks = storage.list_contents(
            self.storage_path(self.task_name_prefix))

        assert all(x in self.submitted_tasks for x in completed_tasks)
        return set(completed_tasks)

    def get_running_tasks(self):
        completed_tasks = self.get_completed_tasks()
        return set(self.submitted_tasks).difference(completed_tasks)

    def wait(self, poll_seconds=5.0):
        """
        Run all tasks to completion.
        """

        completed_tasks = self.get_completed_tasks()
        running_tasks = set(self.submitted_tasks).difference(completed_tasks)

        while True:
            tasks_to_submit = max(
                0,
                self.max_simultaneous_tasks -
                len(self.get_running_tasks()))
            if tasks_to_submit == 0:
                time.sleep(poll_seconds)
                continue

            logging.info("Submitting %d tasks" % len(tasks_to_submit))
            if not all(self.submit_one_task() for _ in range(tasks_to_submit)):
                # We've submitted all our tasks.
                while True:
                    running_tasks = self.get_running_tasks()
                    if not running_tasks:
                        return
                    logging.info("Waiting for %d tasks to complete: %s" % (
                        len(running_tasks),
                        " ".join(running_tasks)))
                    time.sleep(poll_seconds)

    def results(self):
        if self.get_running_tasks():
            raise RuntimeError("Not all tasks have completed")
        for task_name in self.submitted_tasks:
            data_file = self.storage_path(task_name)
            handle = storage.get(data_file)
            value = loads(handle)
            storage.delete(data_file)
            yield value
