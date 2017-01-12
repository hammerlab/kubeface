import logging
import subprocess
from copy import copy

from .backend import Backend
from .worker_configuration import (
    WorkerConfiguration,
    DEFAULT as DEFAULT_WORKER_CONFIG
)


class LocalProcessBackend(Backend):
    @staticmethod
    def add_args(parser):
        pass

    @staticmethod
    def from_args(args):
        return LocalProcessBackend(
            worker_configuration=WorkerConfiguration.from_args(args))

    def __init__(self, worker_configuration=DEFAULT_WORKER_CONFIG):
        unsupported_worker_configuration_fields = [
            'image',
            'pip',
            'pip_packages',
            'kubeface_install_command',
        ]
        bad_fields = worker_configuration.non_default_fields().intersection(
            set(unsupported_worker_configuration_fields))
        if bad_fields:
            raise ValueError(
                "LocalProcessBackend does not handle these worker "
                "configuration fields: %s" % ' '.join(bad_fields))
        if worker_configuration.kubeface_install_policy == 'always':
            raise ValueError(
                "LocalProcessBackend does not support worker configurations "
                "with kubeface_install_policy = 'always'")
        self.worker_configuration = copy(worker_configuration)
        self.worker_configuration.kubeface_install_policy = 'never'

    def submit_task(self, task_name, task_input, task_output):
        command = self.worker_configuration.command(task_input, task_output)
        logging.debug("Running task '%s': %s" % (task_name, command))
        return subprocess.Popen(command, shell=True)
