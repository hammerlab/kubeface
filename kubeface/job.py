import logging
import time
import tempfile
from contextlib import closing

import bitmath

from .serialization import load, dump
from . import storage, naming
from .status_writer import DefaultStatusWriter


class Job(object):
    def __init__(
            self,
            backend,
            tasks_iter,
            max_simultaneous_tasks,
            storage_prefix,
            cache_key=None,
            num_tasks=None,
            cleanup=True):

        self.backend = backend
        self.tasks_iter = tasks_iter
        self.max_simultaneous_tasks = max_simultaneous_tasks
        self.storage_prefix = storage_prefix
        self.cache_key = cache_key if cache_key else naming.make_cache_key()
        self.num_tasks = num_tasks
        self.cleanup = cleanup

        self.job_name = naming.make_job_name(self.cache_key)
        self.submitted_tasks = []
        self.reused_tasks = set()
        self.completed_tasks = set()
        self.running_tasks = set()
        self.status_writer = DefaultStatusWriter(storage_prefix, self.job_name)

        self.status_writer.print_info()

        self.static_status_dict = {
            'backend': str(self.backend),
            'job_name': self.job_name,
            'cache_key': self.cache_key,
            'max_simultaneous_tasks': self.max_simultaneous_tasks,
            'num_tasks': self.num_tasks,
            'start_time': time.asctime(),
        }

    def status_dict(self):
        result = dict(self.static_status_dict)
        result["submitted_tasks"] = list(self.submitted_tasks)
        result["completed_tasks"] = list(self.completed_tasks)
        result["running_tasks"] = list(self.running_tasks)
        result['reused_tasks'] = list(self.reused_tasks)
        return result

    def storage_path(self, filename):
        return self.storage_prefix + "/" + filename

    def submit_one_task(self):
        task_name = None
        while task_name is None:
            try:
                task = next(self.tasks_iter)
            except StopIteration:
                return False

            task_name = naming.make_task_name(
                self.cache_key, len(self.submitted_tasks))
            task_output = self.storage_path(naming.task_result_name(task_name))

            if task_name in self.completed_tasks:
                logging.info("Using existing result: %s" % task_output)
                self.reused_tasks.add(task_name)
                self.submitted_tasks.append(task_name)
                task_name = None

        task_input = self.storage_path(naming.task_input_name(task_name))
        with tempfile.TemporaryFile(prefix="kubeface-upload-") as fd:
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
        self.status_writer.update(self.status_dict())
        self.submitted_tasks.append(task_name)
        return True

    def update(self):
        completed_task_result_names = storage.list_contents(
            self.storage_path(
                naming.task_result_prefix(self.cache_key, self.running_tasks)))
        self.completed_tasks.update(
            naming.task_name_from_result_name(x)
            for x in completed_task_result_names)
        self.running_tasks = set(self.submitted_tasks).difference(
            self.completed_tasks)

    def wait(self, poll_seconds=5.0):
        """
        Run all tasks to completion.
        """

        while True:
            self.update()
            tasks_to_submit = max(
                0,
                self.max_simultaneous_tasks -
                len(self.running_tasks))
            if tasks_to_submit == 0:
                time.sleep(poll_seconds)
                continue

            logging.info("Submitting %d tasks" % tasks_to_submit)
            if not all(self.submit_one_task() for _ in range(tasks_to_submit)):
                # We've submitted all our tasks.
                while True:
                    self.update()
                    self.status_writer.update(self.status_dict())
                    if not self.running_tasks:
                        return
                    logging.info("Waiting for %d tasks to complete: %s" % (
                        len(self.running_tasks),
                        " ".join(self.running_tasks)))
                    time.sleep(poll_seconds)

    def results(self):
        self.update()
        if self.running_tasks:
            raise RuntimeError("Not all tasks have completed")
        for task_name in self.submitted_tasks:
            result_file = self.storage_path(naming.task_result_name(task_name))
            with closing(storage.get(result_file)) as handle:
                value = load(handle)
            if self.cleanup:
                storage.delete(result_file)
            yield value
