# Based loosely on dask distributed joblib backend.

from joblib._parallel_backends import ParallelBackendBase
from joblib.parallel import register_parallel_backend

from .client import Client


class KubefaceBackend(ParallelBackendBase):
    def __init__(self, client):
        self.client = client

    def effective_n_jobs(self, n_jobs=1):
        return self.client.available_concurrency()

    def apply_async(self, func, *args, **kwargs):
        callback = kwargs.pop('callback', None)
        kwargs['pure'] = False
        future = self.executor.submit(func, *args, **kwargs)
        self.futures.add(future)

        @gen.coroutine
        def callback_wrapper():
            result = yield _wait([future])
            self.futures.remove(future)
            callback(result)  # gets called in separate thread

        self.executor.loop.add_callback(callback_wrapper)

        future.get = future.result  # monkey patch to achieve AsyncResult API
        return future

    def abort_everything(self, ensure_ready=True):
        # Tell the executor to cancel any task submitted via this instance
        # as joblib.Parallel will never access those results.
        self.executor.cancel(self.futures)
        self.futures.clear()



def configure():
    register_parallel_backend('kubeface', KubefaceBackend)
