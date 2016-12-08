import logging
import subprocess

from .backend import Backend


class LocalProcessBackend(Backend):
    def __init__(self, python_path='/usr/bin/env python'):
        self.python_path = python_path

    def submit_task(self, task_input, task_output):
        args = [
            "_kubeface-run-task",
            task_input,
            task_output
        ]
        logging.debug("Running: %s" % str(args))
        return subprocess.Popen(args)
