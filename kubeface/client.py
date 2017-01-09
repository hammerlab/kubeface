import math
import logging
import os

from .job import Job
from .task import Task
from . import backends, worker_configuration, naming, storage, kubernetes_backend


def run_multiple(function, values):
    return [function(v) for v in values]


class Client(object):
    @staticmethod
    def add_args(parser):
        group = parser.add_argument_group("kubeface client")
        group.add_argument(
            "--max-simultaneous-tasks",
            type=int,
            default=10)
        group.add_argument(
            "--poll-seconds",
            type=float,
            default=30.0)
        group.add_argument(
            "--storage-prefix",
            default=os.environ.get("KUBEFACE_BUCKET", "gs://kubeface"),
            help="Default: %(default)s")
        group.add_argument(
            "--cache-key-prefix")
        group.add_argument(
            "--never-cleanup",
            action="store_true",
            default=False)
        group.add_argument(
            "--wait-to-raise-task-exception",
            action="store_true",
            default=False)
        group.add_argument(
            "--speculation-percent",
            type=float,
            default=20)
        group.add_argument(
            "--speculation-runtime-percentile",
            type=float,
            default=99)
        group.add_argument(
            "--speculation-max-reruns",
            type=int,
            default=1)

        worker_configuration.WorkerConfiguration.add_args(group)
        backends.add_args(group)

    @staticmethod
    def from_args(args):
        # make sure user isn't trying to call a kubernetes backend with local storage
        if args.backend == 'kubernetes' and \
                not storage.is_google_storage_bucket(args.storage_prefix):
                
            raise ValueError('Cannot use kubernetes backend with local storage')

        backend = backends.backend_from_args(args)
        return Client(
            backend,
            max_simultaneous_tasks=args.max_simultaneous_tasks,
            poll_seconds=args.poll_seconds,
            storage_prefix=args.storage_prefix,
            cache_key_prefix=args.cache_key_prefix,
            never_cleanup=args.never_cleanup,
            wait_to_raise_task_exception=args.wait_to_raise_task_exception,
            speculation_percent=args.speculation_percent,
            speculation_runtime_percentile=args.speculation_runtime_percentile,
            speculation_max_reruns=args.speculation_max_reruns)

    def __init__(
            self,
            backend,
            max_simultaneous_tasks=10,
            poll_seconds=30.0,
            storage_prefix="gs://kubeface",
            cache_key_prefix=None,
            never_cleanup=False,
            wait_to_raise_task_exception=False,
            speculation_percent=0,
            speculation_runtime_percentile=99,
            speculation_max_reruns=1):

        self.backend = backend
        self.max_simultaneous_tasks = max_simultaneous_tasks
        self.poll_seconds = poll_seconds
        self.storage_prefix = storage_prefix
        self.cache_key_prefix = (
            cache_key_prefix if cache_key_prefix
            else naming.make_cache_key_prefix())
        self.never_cleanup = never_cleanup
        self.wait_to_raise_task_exception = wait_to_raise_task_exception
        self.speculation_percent = speculation_percent
        self.speculation_runtime_percentile = speculation_runtime_percentile
        self.speculation_max_reruns = speculation_max_reruns

        self.submitted_jobs = []

    def next_cache_key(self):
        return "%s-%03d" % (
            self.cache_key_prefix,
            len(self.submitted_jobs))

    def submit(self, tasks, num_tasks=None, cache_key=None):
        if num_tasks is None:
            try:
                num_tasks = len(tasks)
            except TypeError:
                pass
        job = Job(
            self.backend,
            tasks,
            num_tasks=num_tasks,
            cache_key=cache_key if cache_key else self.next_cache_key(),
            max_simultaneous_tasks=self.max_simultaneous_tasks,
            storage_prefix=self.storage_prefix,
            wait_to_raise_task_exception=self.wait_to_raise_task_exception,
            speculation_percent=self.speculation_percent,
            speculation_runtime_percentile=self.speculation_runtime_percentile,
            speculation_max_reruns=self.speculation_max_reruns)
        self.submitted_jobs.append(job)
        return job

    def map(
            self,
            function,
            iterable,
            items_per_task=1,
            batched=False,
            num_items=None,
            cache_key=None):
        def grouped():
            iterator = iter(iterable)
            while True:
                items = []
                try:
                    while len(items) < items_per_task:
                        items.append(next(iterator))
                except StopIteration:
                    pass
                if items:
                    yield items
                else:
                    break

        num_tasks = None
        if num_items is None:
            try:
                num_items = len(iterable)
                num_tasks = int(math.ceil(float(num_items) / items_per_task))
            except TypeError:
                pass

        if batched:
            tasks = (Task(function, [values]) for values in grouped())
        else:
            tasks = (
                Task(run_multiple, (function, values)) for values in grouped())
        job = self.submit(tasks, num_tasks=num_tasks, cache_key=cache_key)
        try:
            job.wait(poll_seconds=self.poll_seconds)
            for result in job.results():
                result.log()
                result.raise_if_exception()
                for result_item in result.return_value:
                    yield result_item
        finally:
            self.mark_jobs_done(job_names=[job.job_name])

    def mark_jobs_done(self, job_names=None):
        status_pages = set()
        status_prefixes = naming.status_prefixes(job_names=job_names)
        for prefix in status_prefixes:
            status_pages.update(storage.list_contents(
                self.storage_prefix + "/" + prefix))
        for source_object in status_pages:
            parsed = naming.JOB_STATUS_PAGE.make_tuple(source_object)
            if parsed.status == 'active':
                new_parsed = parsed._replace(status="done")
                dest_object = naming.JOB_STATUS_PAGE.make_string(new_parsed)
                logging.info("Marking job '%s' done: renaming %s -> %s" % (
                    parsed.job_name,
                    source_object,
                    dest_object))
                storage.move(
                    self.storage_prefix + "/" + source_object,
                    self.storage_prefix + "/" + dest_object)
            else:
                logging.info("Already marked done: %s" % source_object)

    def cleanup_job(self, job_name):
        cache_key = naming.JOB.make_tuple(job_name).cache_key
        results = storage.list_contents(
            self.storage_prefix +
            "/" +
            naming.task_result_prefix(cache_key))
        inputs = storage.list_contents(
            self.storage_prefix +
            "/" +
            naming.task_input_prefix(cache_key))
        logging.info("Cleaning up cache key '%s': %d results, %d inputs." % (
            cache_key, len(results), len(inputs)))

        for item in results + inputs:
            storage.delete(self.storage_prefix + "/" + item)

        self.mark_jobs_done(job_names=[job_name])

    def job_summary(self, job_names=None, include_done=False):
        prefixes = naming.status_prefixes(
            job_names=job_names,
            formats=["json"],
            statuses=(["active"] + (["done"] if include_done else [])))
        all_objects = []
        for prefix in prefixes:
            all_objects.extend(
                storage.list_contents(
                    self.storage_prefix + "/" + prefix))
        logging.debug("Listed %d status pages from prefixes: %s" % (
            len(all_objects), " ".join(prefixes)))
        return [
            naming.JOB_STATUS_PAGE.make_tuple(obj)
            for obj in sorted(all_objects)
        ]

    def cleanup(self):
        if self.never_cleanup:
            logging.warn("Cleanup disabled; skipping.")
        else:
            for job in self.submitted_jobs:
                logging.info("Cleaning up for job: %s" % job.job_name)
                self.cleanup_job(job.job_name)
