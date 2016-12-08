import time
import socket
import logging


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
        except Exception as e:
            logging.warn("Task execution raised exception: %s" % e)
            result["exception"] = e
        result["end_time"] = time.time()
        return result
