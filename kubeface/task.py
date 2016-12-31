import time
import socket
import logging
import types
import traceback
import sys


class Task(object):
    def __init__(self, function, args=(), kwargs={}):
        self.function = function
        self.args = args
        self.kwargs = kwargs

    def run(self):
        result = {
            "start_time": time.time(),
            "end_time": None,
            "hostname": socket.gethostname(),
            "exception": None,
            "return_value": None,
        }
        try:
            result["return_value"] = self.function(*self.args, **self.kwargs)
            if isinstance(result["return_value"], types.GeneratorType):
                result["return_value"] = list(result["return_value"])
        except Exception as e:
            traceback_string = traceback.format_exc()
            logging.warn("Task execution raised exception: %s. %s" % (
                e, traceback_string))
            result["exception"] = e
            result["exception_traceback_string"] = traceback_string
            result["return_value"] = None

            # To make it easier to debug issues, we include some process info
            # when there is an exception.
            result["invocation_args"] = sys.argv
            result["python_version"] = sys.version

        result["end_time"] = time.time()
        return result
