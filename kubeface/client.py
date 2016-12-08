from .job import Job
from .task import Task
from . import backends


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
            default=5.0)
        parser.add_argument(
            "--storage-prefix",
            default="gs://kubeface")

        backends.add_args(parser)

    @staticmethod
    def from_args(args):
        backend = backends.backend_from_args(args)
        return Client(
            backend,
            max_simultaneous_tasks=args.max_simultaneous_tasks,
            poll_seconds=args.poll_seconds,
            storage_prefix=args.storage_prefix)

    def __init__(
            self,
            backend,
            max_simultaneous_tasks=10,
            poll_seconds=5.0,
            storage_prefix="gs://kubeface"):

        self.backend = backend
        self.max_simultaneous_tasks = max_simultaneous_tasks
        self.poll_seconds = poll_seconds
        self.storage_prefix = storage_prefix

    def submit(self, tasks):
        return Job(
            self.backend,
            tasks,
            max_simultaneous_tasks=self.max_simultaneous_tasks,
            storage_prefix=self.storage_prefix)

    def map(self, function, iterable):
        tasks = (Task(function, (i,)) for i in iterable)
        job = self.submit(tasks)
        job.wait(poll_seconds=self.poll_seconds)
        for result in job.results():
            if result['exception']:
                raise result['exception']
            yield result['return_value']
