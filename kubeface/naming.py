import socket
from datetime import datetime
import getpass
import hashlib
import time
from os.path import commonprefix


def hash_value(s, characters=8):
    return hashlib.sha1(str(s).encode()).hexdigest()[:characters]


def make_cache_key():
    cache_key = "%s-%s-%s-%s" % (
        socket.gethostname()[:8],
        getpass.getuser(),
        datetime.strftime(datetime.now(), "%Y-%m-%d-%H:%M:%S"),
        hash_value(time.time()))
    return cache_key


def make_job_name(cache_key):
    return cache_key + "::" + hash_value(time.time())


def make_task_name(cache_key, task_num):
    return "%s::%d" % (cache_key, task_num)


def task_result_prefix(cache_key, task_names=[]):
    prefix = "result::" + cache_key
    if task_names:
        better_prefix = commonprefix([
            task_result_name(t) for t in task_names
        ])
        assert better_prefix.startswith(prefix)
        return better_prefix
    return prefix


def task_input_name(task_name):
    return "input::" + task_name


def task_result_name(task_name):
    return "result::" + task_name


def status_name(job_name, fmt, is_active):
    active = "active" if is_active else "done"
    return "%s::%s::%s.%s" % (active, fmt, job_name, fmt)


def task_name_from_result_name(task_result_name):
    if not task_result_name.startswith("result::"):
        raise ValueError("Not a result: %s" % task_result_name)
    return task_result_name[len("result::"):]


def task_name_from_input_name(task_input_name):
    if not task_result_name.startswith("input::"):
        raise ValueError("Not an input: %s" % task_input_name)
    return task_result_name[len("input::"):]


def cache_key_from_task_name(task_name):
    return task_name.split("::")[-2]


def task_num_from_task_name(task_name):
    return int(task_name.split("::")[-1])


def sanitize(name):
    return (
        name
        .replace(".", "-")
        .replace(":", "-")
        .replace("_", "-").lower())
