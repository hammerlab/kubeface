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
        volume_mounts = []  # pairs of (host path, docker path)
        if not task_input.startswith("gs://"):
            # Using a local filesystem as storage, so we'll want to
            # mount it on the docker image.
            data_dir = os.path.dirname(task_input)
            assert os.path.dirname(task_output) == data_dir

            task_input = os.path.join(
                DOCKER_MOUNT, os.path.basename(task_input))
            task_output = os.path.join(
                DOCKER_MOUNT, os.path.basename(task_output))
            volume_mounts.append((data_dir, DOCKER_MOUNT))

        # We also mount the kubeface package directory, so it can
        # installed on the docker image if desired.
        kubeface_package_dir = os.path.abspath(
            os.path.join(
                os.path.dirname(__file__),
                ".."))
        volume_mounts.append((kubeface_package_dir, KUBEFACE_MOUNT))

        volume_mount_args = []
        for (host_path, docker_path) in volume_mounts:
            volume_mount_args.append("-v")
            volume_mount_args.append("%s:%s" % (host_path, docker_path))

        command = (
            shlex.split(self.docker_command) +
            ["run"] +
            volume_mount_args +
            [
                self.worker_configuration.image,
                "sh",
                "-c",
                self.worker_configuration.command(task_input, task_output),
            ]
        )
        logging.info("Running task '%s': %s" % (task_name, str(command)))
        return subprocess.Popen(command)

    @staticmethod
    def supports_storage_prefix(storage_prefix):
        # docker backends can work with any kind of storage
        return True
