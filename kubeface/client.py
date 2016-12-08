from .job import Job


class Client(object):
    def __init__(
            self,
            backend,
            max_simultaneous_tasks=10,
            storage_prefix="gs://kubeface"):

        self.backend = backend
        self.max_simultaneous_tasks = max_simultaneous_tasks
        self.storage_prefix = storage_prefix

    def submit(self, tasks):
        return Job(
            self.backend,
            tasks,
            max_simultaneous_tasks=self.max_simultaneous_tasks,
            storage_prefix=self.storage_prefix)
