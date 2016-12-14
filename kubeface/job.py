import logging
import time
import tempfile
from contextlib import closing

import bitmath

from .serialization import load, dump
from . import storage, naming


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

        self.name = naming.make_job_name()
        self.submitted_tasks = []

    def storage_path(self, filename):
        return self.storage_prefix + "/" + filename

    def submit_one_task(self):
        try:
            task = next(self.tasks_iter)
        except StopIteration:
            return False

        task_name = naming.make_task_name(self.name, len(self.submitted_tasks))
        task_input = self.storage_path(naming.task_input_name(task_name))
        task_output = self.storage_path(naming.task_result_name(task_name))

        with tempfile.TemporaryFile(prefix="kubeface-job-task-upload-") as fd:
            dump(task, fd)
            size_string = (
                bitmath.Byte(bytes=fd.tell())
                .best_prefix()
                .format("{value:.2f} {unit}"))
            logging.info("Uploading: %s [%s] for task %s" % (
                task_input,
                size_string,
                task_name))
            fd.seek(0)
            storage.put(task_input, fd)
        self.backend.submit_task(task_name, task_input, task_output)
        self.submitted_tasks.append(task_name)
        return True

    def get_completed_tasks(self):
        completed_task_result_names = storage.list_contents(
            self.storage_path(naming.task_result_prefix(self.name)))
        completed_tasks = set(
            naming.task_name_from_result_name(x)
            for x in completed_task_result_names)

        assert all(x in self.submitted_tasks for x in completed_tasks)
        return completed_tasks

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

            logging.info("Submitting %d tasks" % tasks_to_submit)
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
            result_file = self.storage_path(naming.task_result_name(task_name))
            with closing(storage.get(result_file)) as handle:
                value = load(handle)
            storage.delete(result_file)
            yield value
