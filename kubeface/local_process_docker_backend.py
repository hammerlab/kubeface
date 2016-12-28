import logging
import subprocess
import shlex
import os

from .backend import Backend
from .worker_configuration import (
    WorkerConfiguration,
    DEFAULT as DEFAULT_WORKER_CONFIG
)

DOCKER_MOUNT = "/kubeface-data"
KUBEFACE_MOUNT = "/kubeface-package"


class LocalProcessDockerBackend(Backend):
    @staticmethod
    def add_args(parser):
        parser.add_argument(
            "--local-process-docker-command",
            default="docker")

    @staticmethod
    def from_args(args):
        return LocalProcessDockerBackend(
            worker_configuration=WorkerConfiguration.from_args(args),
            docker_command=args.local_process_docker_command)

    def __init__(
            self,
            worker_configuration=DEFAULT_WORKER_CONFIG,
            docker_command="docker"):
        self.worker_configuration = worker_configuration
        self.docker_command = docker_command

    def submit_task(self, task_name, task_input, task_output):
        if not task_input.startswith("gs://"):
            data_dir = os.path.dirname(task_input)
            assert os.path.dirname(task_output) == data_dir

            task_input = os.path.join(
                DOCKER_MOUNT, os.path.basename(task_input))
            task_output = os.path.join(
                DOCKER_MOUNT, os.path.basename(task_output))

        kubeface_package_dir = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                ".."))

        worker_command = self.worker_configuration.command(
            task_input,
            task_output)
        command = shlex.split(self.docker_command) + [
            "run",
            "-v",
            "%s:%s" % (data_dir, DOCKER_MOUNT),
            "-v",
            "%s:%s" % (kubeface_package_dir, KUBEFACE_MOUNT),
            self.worker_configuration.image,
            "sh",
            "-c",
            worker_command,
        ]
        logging.info("Running task '%s': %s" % (task_name, str(command)))
        print(command)
        return subprocess.Popen(command)
