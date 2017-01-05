import sys
import socket
import logging
import time
import platform
from datetime import timedelta
from contextlib import closing

from . import storage
from .serialization import load
from .common import human_readable_memory_size


def get_process_info():
    # For debugging we record some process info in results.
    return {
        'invocation_args': sys.argv,
        'python_version': sys.version,
        'hostname': socket.gethostname(),
        'platform': platform.platform(),
    }


class Result(object):
    @staticmethod
    def from_storage(storage_path):
        with closing(storage.get(storage_path)) as handle:
            value = load(handle)
            assert isinstance(value, Result), type(value)
            value.serialization_info["storage_path"] = storage_path
            value.serialization_info["result_bytes"] = handle.tell()
        return value

    def __init__(
            self,
            start_time,
            end_time,
            input_size=None,
            exception=None,
            exception_traceback_string=None,
            return_value=None,
            process_info=get_process_info()):
        self.input_size = input_size
        self.start_time = start_time
        self.end_time = end_time
        self.exception = exception
        self.exception_traceback_string = exception_traceback_string
        self.return_value = return_value
        self.process_info = process_info

        if exception is not None:
            assert return_value is None
            assert exception_traceback_string is not None
            self.result_type = "exception"
        else:
            self.result_type = "value"

        self.serialization_info = {}  # set upon deserialization

    def run_seconds(self):
        return self.end_time - self.start_time

    def description(self, indent=""):
        fields = [
            ("result type", self.result_type),
            ("start time", time.asctime(time.localtime(self.start_time))),
            ("run time", str(timedelta(seconds=self.run_seconds()))),
            ("hostname", self.process_info['hostname']),
            ("platform", self.process_info['platform']),
            ("python version", self.process_info['python_version']),
            ("invocation arguments", "\n".join(
                self.process_info['invocation_args'])),
        ]
        if self.input_size:
            fields.append(
                ("input size", human_readable_memory_size(self.input_size)))
        if 'result_bytes' in self.serialization_info:
            fields.append(
                ("result size",
                    human_readable_memory_size(
                        self.serialization_info['result_bytes'])))

        if self.result_type == 'value':
            fields.append(("return value type", str(type(self.return_value))))
        else:
            fields.extend([
                ("exception", str(self.exception)),
                ("traceback", self.exception_traceback_string),
            ])

        max_header_length = max(len(pair[0]) for pair in fields)
        row_template = "%" + str(max_header_length) + "s : %s"

        def format_value(s):
            return s.replace("\n", "\n" + "   " + " " * max_header_length)

        return (
            "\n" +
            "\n".join(
                row_template % (key, format_value(value))
                for (key, value) in fields)
        ).replace("\n", "\n" + indent)

    def log(self):
        indent = " *  "
        if self.result_type == 'value':
            logging.debug("Result (success): %s" % (
                self.description(indent=indent)))
        else:
            logging.error("Result (exception): %s" % (
                self.description(indent=indent)))

    def raise_if_exception(self):
        if self.result_type == 'exception':
            logging.error("Re-raising exception for task.")
            raise self.exception
