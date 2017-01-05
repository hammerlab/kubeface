import time
import logging
import types
import traceback

from .result import Result


class Task(object):
    def __init__(self, function, args=(), kwargs={}):
        self.function = function
        self.args = args
        self.kwargs = kwargs

    def run(self, input_size=None):
        start_time = time.time()
        exception = None
        exception_traceback_string = None

        try:
            return_value = self.function(*self.args, **self.kwargs)
            if isinstance(return_value, types.GeneratorType):
                return_value = list(return_value)
        except Exception as e:
            traceback_string = traceback.format_exc()
            logging.warn("Task execution raised exception: %s. %s" % (
                e, traceback_string))
            exception = e
            exception_traceback_string = traceback_string
            return_value = None

        return Result(
            start_time=start_time,
            end_time=time.time(),
            exception=exception,
            exception_traceback_string=exception_traceback_string,
            return_value=return_value,
            input_size=input_size)
