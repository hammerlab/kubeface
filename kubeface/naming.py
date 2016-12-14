import socket
from datetime import datetime
import getpass
import hashlib
import time


def hash_value(s, characters=8):
    return hashlib.sha1(str(s).encode()).hexdigest()[:characters]


def make_job_name():
    job_name = "%s-%s-%s-%s" % (
        socket.gethostname()[:8],
        getpass.getuser(),
        datetime.strftime(datetime.now(), "%Y-%m-%d-%H:%M:%S"),
        hash_value(time.time()))
    return job_name


def task_result_prefix(job_name):
    return "result::" + job_name


def make_task_name(job_name, task_num):
    return "%s::%d" % (job_name, task_num)


def task_input_name(task_name):
    return "input::" + task_name


def task_result_name(task_name):
    return "result::" + task_name


def task_name_from_result_name(task_result_name):
    if not task_result_name.startswith("result::"):
        raise ValueError("Not a result: %s" % task_result_name)
    return task_result_name[len("result::"):]


def task_name_from_input_name(task_input_name):
    if not task_result_name.startswith("input::"):
        raise ValueError("Not an input: %s" % task_input_name)
    return task_result_name[len("input::"):]


def job_name_from_task_name(task_name):
    return task_name.split("::")[-2]


def task_num_from_task_name(task_name):
    return int(task_name.split("::")[-1])


def sanitize(name):
    return (
        name
        .replace(".", "-")
        .replace(":", "-")
        .replace("_", "-").lower())
