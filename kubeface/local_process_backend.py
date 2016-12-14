import logging
import subprocess

from .backend import Backend


def run_task_args(task_input, task_output, delete_input):
    args = [
        "_kubeface-run-task",
        task_input,
        task_output,
        "--verbose",
    ]
    if delete_input:
        args.append("--delete-input")
    return args


class LocalProcessBackend(Backend):
    @staticmethod
    def add_args(parser):
        parser.add_argument(
            "--local-process-keep-input",
            dest="local_process_delete_input",
            action="store_false",
            default=True)

    @staticmethod
    def from_args(args):
        return LocalProcessBackend(
            delete_input=args.local_process_delete_input)

    def __init__(self, delete_input=True):
        self.delete_input = delete_input

    def submit_task(self, task_name, task_input, task_output):
        args = run_task_args(
            task_input,
            task_output,
            delete_input=self.delete_input)
        logging.debug("Running task '%s': %s" % (task_name, str(args)))
        return subprocess.Popen(args)
