import logging
import subprocess

from .backend import Backend


class LocalProcessBackend(Backend):
    @staticmethod
    def add_args(parser):
        pass

    @staticmethod
    def from_args(args):
        return LocalProcessBackend()

    def __init__(self):
        pass

    def run_task_args(self, task_input, task_output, delete_input):
        args = [
            "_kubeface-run-task",
            task_input,
            task_output
        ]
        if delete_input:
            args.append("--delete-input")
        return args

    def submit_task(self, task_input, task_output, delete_input=True):
        args = self.run_task_args(
            task_input,
            task_output,
            delete_input=delete_input)
        logging.debug("Running: %s" % str(args))
        return subprocess.Popen(args)
