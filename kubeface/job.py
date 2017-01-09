import logging
import time
import tempfile
import collections

from numpy import percentile, mean

from .serialization import dump
from . import storage, naming
from .status_writer import DefaultStatusWriter
from .common import human_readable_memory_size
from .result import Result


class Job(object):
    def __init__(
            self,
            backend,
            tasks_iter,
            max_simultaneous_tasks,
            storage_prefix,
            cache_key,
            num_tasks=None,
            wait_to_raise_task_exception=False,
            speculation_percent=0,
            speculation_runtime_percentile=99,
            speculation_max_reruns=0):

        self.backend = backend
        self.tasks_iter = tasks_iter
        self.max_simultaneous_tasks = max_simultaneous_tasks
        self.storage_prefix = storage_prefix
        self.cache_key = cache_key
        self.num_tasks = num_tasks
        self.wait_to_raise_task_exception = wait_to_raise_task_exception
        self.speculation_percent = speculation_percent
        self.speculation_runtime_percentile = speculation_runtime_percentile
        self.speculation_max_reruns = speculation_max_reruns

        self.job_name = naming.make_job_name(self.cache_key)
        self.task_queue_times = collections.defaultdict(list)
        self.submitted_tasks = []
        self.reused_tasks = set()
        self.completed_tasks = {}
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

    def submit_task(self, task_name):
        queue_time = int(time.time())
        task_result_template = self.storage_path(
            naming.TASK_RESULT.template.format(
                task_name=task_name,
                attempt_num=len(self.task_queue_times[task_name]),
                queue_time=queue_time,
                result_type="{result_type}",   # filled in by worker
                result_time="{result_time}"))  # filled in by worker

        task_input = self.storage_path(
            naming.TASK_INPUT.make_string(task_name=task_name))

        self.backend.submit_task(task_name, task_input, task_result_template)
        self.status_writer.update(self.status_dict())
        self.submitted_tasks.append(task_name)
        self.task_queue_times[task_name].append(queue_time)

    def submit_next_task(self):
        task_name = None
        while task_name is None:
            try:
                task = next(self.tasks_iter)
            except StopIteration:
                return False

            task_name = naming.TASK.make_string(
                cache_key=self.cache_key,
                task_num=len(self.submitted_tasks))

            if task_name in self.completed_tasks:
                completed_task_info = self.completed_tasks[task_name]
                logging.info("Using existing result: %s" % (
                    completed_task_info['task_result_name']))
                self.reused_tasks.add(task_name)
                self.submitted_tasks.append(task_name)
                task_name = None

        task_input = self.storage_path(
            naming.TASK_INPUT.make_string(task_name=task_name))
        with tempfile.TemporaryFile(prefix="kubeface-upload-") as fd:
            dump(task, fd)
            size_string = human_readable_memory_size(fd.tell())
            logging.info("Uploading: %s [%s] for task %s" % (
                task_input,
                size_string,
                task_name))
            fd.seek(0)
            storage.put(task_input, fd)

        self.submit_task(task_name)
        return True

    def update(self):
        completed_task_result_names = storage.list_contents(
            self.storage_path(
                naming.task_result_prefix(self.cache_key, self.running_tasks)))
        for completed_task_result_name in completed_task_result_names:
            info = naming.TASK_RESULT.make_tuple(completed_task_result_name)
            if info.task_name not in self.completed_tasks:
                if info.result_type == 'exception':
                    result = Result.from_storage(
                        self.storage_path(completed_task_result_name))
                    result.log()
                    if self.wait_to_raise_task_exception:
                        logging.warning(
                            "Waiting for other tasks to run before raising "
                            "exception.")
                    else:
                        result.raise_if_exception()
                        assert False
                self.completed_tasks[info.task_name] = {
                    'parsed_result_name': info,
                    'task_result_name': completed_task_result_name,
                }

        self.running_tasks = set(self.submitted_tasks).difference(
            set(self.completed_tasks))

    def tasks_elegible_for_speculation(self, speculation_runtime_threshold):
        # Consider speculating.
        elegible_tasks_by_runtime = [
            task_name
            for task_name in self.running_tasks
            if (
                self.task_queue_times[task_name][-1] >
                speculation_runtime_threshold)
        ]
        elegible_tasks = [
            task_name
            for task_name in elegible_tasks_by_runtime
            if (
                len(self.task_queue_times[task_name]) <
                self.speculation_max_reruns)
        ]
        logging.info(
            "%d tasks could be speculatively rerun based "
            "on a queue time threshold of %0.2f sec; of "
            "these %d are elegible because they have not "
            "been run more than %d times." % (
                len(elegible_tasks_by_runtime),
                speculation_runtime_threshold,
                len(elegible_tasks),
                self.speculation_max_reruns))
        return elegible_tasks

    def wait(self, poll_seconds=5.0):
        """
        Run all tasks to completion.

        Speculation algorithm:
            - No speculation occurs until all tasks have been submitted and at
              least 100 - speculation_percent tasks have completed.
            - Once this threshold is reached, tasks are rerun in order, i.e.
              based how long they have been queued.
            - A task will be rerun when its queue time exceeds
              speculation_runtime_percentile of the queue times of the
              tasks that completed successfully without speculation. This will
              reset its queue time to 0.
            - Tasks can be rerun up to speculation_max_reruns times.
            - We are still limited by max_simultaneous_tasks. If more than this
              number of tasks fail, we won't be able to recover.
        """

        while True:
            self.update()
            num_to_submit = max(
                0,
                self.max_simultaneous_tasks -
                len(self.running_tasks))
            if num_to_submit == 0:
                time.sleep(poll_seconds)
                continue

            logging.info("Submitting %d tasks" % num_to_submit)
            if not all(self.submit_next_task() for _ in range(num_to_submit)):
                # We've submitted all our tasks.
                speculation_runtime_threshold = None
                while True:
                    self.update()
                    self.status_writer.update(self.status_dict())
                    if not self.running_tasks:
                        return

                    if speculation_runtime_threshold is None:
                        percent_tasks_running = (
                            len(self.running_tasks) * 100.0 /
                            len(self.submitted_tasks))
                        if percent_tasks_running < self.speculation_percent:
                            elapsed_times = [
                                int(t["parsed_result_name"].result_time)
                                for t in self.completed_tasks.values()
                            ]
                            speculation_runtime_threshold = percentile(
                                elapsed_times,
                                self.speculation_runtime_percentile)
                            logging.info(
                                "Enabling speculation: %0.2ff%% of tasks "
                                "running. "
                                "Task queue times (sec): "
                                "min=%0.1f mean=%0.1f max=%0.1f. Queue time "
                                "threshold for resubmitting tasks will be "
                                "%0.0f percentile of these times, which is "
                                "%0.2f" % (
                                    percent_tasks_running,
                                    min(elapsed_times),
                                    mean(elapsed_times),
                                    max(elapsed_times),
                                    self.speculation_runtime_percentile,
                                    speculation_runtime_threshold))

                    if speculation_runtime_threshold is not None:
                        elegible_tasks = self.tasks_elegible_for_speculation(
                            speculation_runtime_threshold)

                        if elegible_tasks:
                            capacity = max(
                                0,
                                self.max_simultaneous_tasks - sum(
                                    len(self.task_queue_times[task_name])
                                    for task_name in self.running_tasks))
                            to_speculate = elegible_tasks[:capacity]
                            logging.info(
                                "Capacity for re-running up to %d tasks. "
                                "Will speculatively re-run %d tasks." % (
                                    len(to_speculate)))
                            for task_name in to_speculate:
                                self.submit_task(task_name)

                    logging.info("Waiting for %d tasks to complete: %s" % (
                        len(self.running_tasks),
                        " ".join(self.running_tasks)))
                    time.sleep(poll_seconds)

    def results(self):
        self.update()
        if self.running_tasks:
            raise RuntimeError("Not all tasks have completed")
        for task_name in self.submitted_tasks:
            result_file = self.storage_path(
                self.completed_tasks[task_name]['task_result_name'])
            result = Result.from_storage(result_file)
            yield result
