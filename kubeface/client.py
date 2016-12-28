import math
import logging
import collections

from .job import Job
from .task import Task
from . import backends, worker_configuration, naming, storage


def run_multiple(function, values):
    return [function(v) for v in values]


JobSummary = collections.namedtuple(
    "JobSummary",
    "job_name cache_key status_kind status_object")


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
            default="gs://kubeface")
        group.add_argument(
            "--cache-key-prefix")
        group.add_argument(
            "--fail-on-worker-exception",
            choices=('sooner', 'later'),
            default=["sooner"])
        group.add_argument(
            "--never-cleanup",
            action="store_true",
            default=False)

        worker_configuration.WorkerConfiguration.add_args(parser)
        backends.add_args(parser)

    @staticmethod
    def from_args(args):
        backend = backends.backend_from_args(args)
        return Client(
            backend,
            max_simultaneous_tasks=args.max_simultaneous_tasks,
            poll_seconds=args.poll_seconds,
            storage_prefix=args.storage_prefix,
            cache_key_prefix=args.cache_key_prefix,
            never_cleanup=args.never_cleanup)

    def __init__(
            self,
            backend,
            max_simultaneous_tasks=10,
            poll_seconds=30.0,
            storage_prefix="gs://kubeface",
            cache_key_prefix=None,
            never_cleanup=False):

        self.backend = backend
        self.max_simultaneous_tasks = max_simultaneous_tasks
        self.poll_seconds = poll_seconds
        self.storage_prefix = storage_prefix
        self.cache_key_prefix = (
            cache_key_prefix if cache_key_prefix
            else naming.make_cache_key_prefix())
        self.never_cleanup = never_cleanup

        self.submitted_jobs = []

    def next_cache_key(self):
        return "%s-%03d" % (
            self.cache_key_prefix,
            len(self.submitted_jobs))

    def submit(self, tasks, num_tasks=None):
        if num_tasks is None:
            try:
                num_tasks = len(tasks)
            except TypeError:
                pass
        job = Job(
            self.backend,
            tasks,
            num_tasks=num_tasks,
            cache_key=self.next_cache_key(),
            max_simultaneous_tasks=self.max_simultaneous_tasks,
            storage_prefix=self.storage_prefix,
            cleanup=self.cleanup)
        self.submitted_jobs.append(job)
        return job

    def map(
            self,
            function,
            iterable,
            items_per_task=1,
            batched=False,
            num_items=None):
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
        job = self.submit(tasks, num_tasks=num_tasks)
        job.wait(poll_seconds=self.poll_seconds)
        for result in job.results():
            if result['exception']:
                raise result['exception']
            for result_item in result['return_value']:
                yield result_item

    def cleanup_job(self, job_name):
        cache_key = naming.cache_key_from_job_name(job_name)
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
            storage.delete(item)

        status_pages = set()
        for prefix in naming.status_prefixes(job_name):
            status_pages.update(storage.list_contents(prefix))
        for source_object in status_pages:
            parsed = naming.parse_status_name(source_object)
            assert parsed['is_active']
            parsed['is_active'] = False
            dest_object = naming.status_name(**parsed)
            logging.info("Cleaning up job '%s': renaming %s -> %s" % (
                job_name,
                source_object,
                dest_object))
            storage.move(
                self.storage_prefix + "/" + source_object,
                self.storage_prefix + "/" + dest_object)

    def job_summary(self, job_names=None):
        prefixes = naming.status_prefixes(job_names=job_names)
        all_objects = []
        for prefix in prefixes:
            all_objects.extend(
                storage.list_contents(
                    self.storage_prefix + "/" + prefix))
        logging.debug("Listed %d status pages from prefixes: %s" % (
            len(all_objects), " ".join(prefixes)))
        results = collections.OrderedDict()

        for obj in sorted(all_objects):
            parsed = naming.parse_status_name(obj)
            cache_key = naming.cache_key_from_job_name(parsed['job_name'])
            if cache_key not in results:
                results[cache_key] = []
            results[cache_key].append(parsed)

        return results

    def cleanup(self):
        if self.never_cleanup:
            logging.warn("Cleanup disabled; skipping.")
            return
        for job in self.submitted_jobs:
            logging.info("Cleaning up for job: %s" % job.job_name)
            self.cleanup_job(job)
