import tempfile
import json
import logging
import subprocess
import time

from .backend import Backend
from .worker_configuration import WorkerConfiguration
from .common import check_call
from .storage import is_google_storage_bucket
from . import naming


class KubernetesBackend(Backend):

    @staticmethod
    def add_args(parser):
        default = KubernetesBackend(worker_configuration=None)
        parser.add_argument(
            "--kubeface-kubernetes-cluster",
            default=default.cluster,
            help="Cluster. Default: %(default)s")
        parser.add_argument(
            "--kubeface-kubernetes-task-resources-cpu",
            default=default.task_resources_cpu,
            type=int,
            help="CPUs per task. Default: %(default)s")
        parser.add_argument(
            "--kubeface-kubernetes-task-resources-memory-mb",
            default=default.task_resources_memory_mb,
            type=float,
            help="Memory (mb) per task. Default: %(default)s")
        parser.add_argument(
            "--kubeface-kubernetes-retries",
            default=default.retries,
            type=int,
            help="Max retries for kubernetes commands. Default: %(default)s")
        parser.add_argument(
            "--kubeface-kubernetes-image-pull-policy",
            default=default.image_pull_policy,
            choices=("Always", "IfNotPresent", "Never"),
            help="Image pull policy. Default: %(default)s")

    @staticmethod
    def from_args(args):
        arg_prefix = "kubeface_kubernetes_"
        return KubernetesBackend(
            worker_configuration=WorkerConfiguration.from_args(args),
            **dict(
                (key[len(arg_prefix):], value)
                for (key, value) in args._get_kwargs()
                if key.startswith(arg_prefix)))

    def __init__(
            self,
            worker_configuration,
            cluster=None,
            task_resources_cpu=1,
            task_resources_memory_mb=1000.0,
            retries=12,
            image_pull_policy='Always'):
        self.worker_configuration = worker_configuration
        self.cluster = cluster
        self.task_resources_cpu = task_resources_cpu
        self.task_resources_memory_mb = task_resources_memory_mb
        self.retries = retries
        self.image_pull_policy = image_pull_policy

    def submit_task(self, task_name, task_input, task_output):
        specification = self.task_specification(
            task_name,
            task_input,
            task_output)
        with tempfile.NamedTemporaryFile(
                mode="w+",
                prefix="kubeface-kubernetes-%s" % task_name,
                suffix=".json") as fd:
            json.dump(specification, fd, indent=4)
            logging.debug(json.dumps(specification, indent=4))
            fd.flush()
            retry_num = 0
            while True:
                try:
                    check_call(["kubectl", "create", "-f", fd.name])
                    return task_name
                except subprocess.CalledProcessError:
                    logging.warn("Error calling kutectl on spec: \n%s" % (
                        json.dumps(specification, indent=4)))
                    retry_num += 1
                    if retry_num >= self.retries:
                        raise
                    sleep_time = 2.0**retry_num
                    logging.info("Retry %d / %d. Sleeping for %0.1f sec." % (
                        retry_num, self.retries, sleep_time))
                    time.sleep(sleep_time)

    def task_specification(self, task_name, task_input, task_output):
        task_info = naming.TASK.make_tuple(task_name)
        logging.info(
            "Generating kubernetes specification for task %d in job %s" % (
                task_info.task_num, task_info.cache_key))

        sanitized_task_name = naming.sanitize(task_name)
        sanitized_cache_key = naming.sanitize(task_info.cache_key)

        result = {
            "kind": "Pod",
            "apiVersion": "v1",
            "metadata": {
                "name": sanitized_task_name,
                "labels": {
                    "kubeface_job": sanitized_cache_key,
                },
                "namespace": "",
            },
            "spec": {
                "containers": [
                    {
                        "name": str(task_info.task_num),
                        "image": self.worker_configuration.image,
                        "imagePullPolicy": self.image_pull_policy,
                        "command": [
                            "sh",
                            "-c",
                            self.worker_configuration.command(
                                task_input,
                                task_output),
                        ],
                        "resources": {
                            "requests": {
                                "cpu": self.task_resources_cpu,
                                "memory": (
                                    "%sMi" %
                                    self.task_resources_memory_mb),
                            },
                        },
                    },
                ],
                "restartPolicy": "Never",
            }
        }
        return result

    @staticmethod
    def supports_storage(path):
        # kubernetes backend requires bucket storage
        return is_google_storage_bucket(path)
