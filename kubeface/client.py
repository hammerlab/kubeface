import math

from .job import Job
from .task import Task
from . import backends


def run_multiple(function, values):
    return [function(v) for v in values]


class Client(object):
    @staticmethod
    def add_args(parser):
        parser.add_argument(
            "--max-simultaneous-tasks",
            type=int,
            default=10)
        parser.add_argument(
            "--poll-seconds",
            type=float,
            default=30.0)
        parser.add_argument(
            "--storage-prefix",
            default="gs://kubeface")
        parser.add_argument(
            "--cache-key")
        parser.add_argument(
            "--no-cleanup",
            action="store_false",
            default=True,
            dest="cleanup")

        backends.add_args(parser)

    @staticmethod
    def from_args(args):
        backend = backends.backend_from_args(args)
        return Client(
            backend,
            max_simultaneous_tasks=args.max_simultaneous_tasks,
            poll_seconds=args.poll_seconds,
            storage_prefix=args.storage_prefix,
            cache_key=args.cache_key,
            cleanup=args.cleanup)

    def __init__(
            self,
            backend,
            max_simultaneous_tasks=10,
            poll_seconds=30.0,
            storage_prefix="gs://kubeface",
            cache_key=None,
            cleanup=True):

        self.backend = backend
        self.max_simultaneous_tasks = max_simultaneous_tasks
        self.poll_seconds = poll_seconds
        self.storage_prefix = storage_prefix
        self.cache_key = cache_key
        self.cleanup = cleanup

        self.num_submitted = 0

        self.backend.cleanup = cleanup

    def next_cache_key(self):
        if not self.cache_key:
            return None
        if self.num_submitted == 0:
            return self.cache_key
        else:
            return "%s%d" % (self.cache_key, self.num_submitted)

    def submit(self, tasks, num_tasks=None):
        if num_tasks is None:
            try:
                num_tasks = len(tasks)
            except TypeError:
                pass
        return Job(
            self.backend,
            tasks,
            num_tasks=num_tasks,
            cache_key=self.next_cache_key(),
            max_simultaneous_tasks=self.max_simultaneous_tasks,
            storage_prefix=self.storage_prefix,
            cleanup=self.cleanup)

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
