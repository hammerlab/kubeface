import logging
import subprocess

from .backend import Backend


def run_task_args(task_input, task_output, cleanup):
    args = [
        "_kubeface-run-task",
        task_input,
        task_output,
        "--verbose",
    ]
    if cleanup:
        args.append("--delete-input")
    return args


class LocalProcessBackend(Backend):
    @staticmethod
    def add_args(parser):
        pass

    @staticmethod
    def from_args(args):
        return LocalProcessBackend()

    def __init__(self, cleanup=True):
        self.cleanup = cleanup

    def submit_task(self, task_name, task_input, task_output):
        args = run_task_args(
            task_input,
            task_output,
            cleanup=self.cleanup)
        logging.debug("Running task '%s': %s" % (task_name, str(args)))
        return subprocess.Popen(args)
