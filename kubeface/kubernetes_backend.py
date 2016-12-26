import tempfile
import json
import logging
import subprocess
import time

from .backend import Backend
from .local_process_backend import run_task_args
from .common import check_call
from . import naming


class KubernetesBackend(Backend):

    @staticmethod
    def add_args(parser):
        parser.add_argument("--kubernetes-image")
        parser.add_argument(
            "--kubernetes-image-pull-policy",
            default="Always")
        parser.add_argument("--kubernetes-cluster")
        parser.add_argument(
            "--kubernetes-task-resources-cpu",
            type=int,
            metavar="N",
            default=1)
        parser.add_argument(
            "--kubernetes-task-resources-memory-mb",
            type=float,
            metavar="X",
            default=1000.0)
        parser.add_argument(
            "--kubernetes-active-deadline-seconds",
            type=int,
            metavar="N",
            default=100)

    @staticmethod
    def from_args(args):
        if not args.kubernetes_image:
            raise ValueError("--kubernetes-image is required")
        return KubernetesBackend(
            image=args.kubernetes_image,
            image_pull_policy=args.kubernetes_image_pull_policy,
            cluster=args.kubernetes_cluster,
            task_resources_cpu=args.kubernetes_task_resources_cpu,
            task_resources_memory_mb=args.kubernetes_task_resources_memory_mb,
            active_deadline_seconds=args.kubernetes_active_deadline_seconds)

    def __init__(
            self,
            image,
            image_pull_policy="Always",
            cluster=None,
            task_resources_cpu=1,
            task_resources_memory_mb=1000,
            active_deadline_seconds=100,
            retries=12,
            cleanup=True):
        self.image = image
        self.image_pull_policy = image_pull_policy
        self.cluster = cluster
        self.task_resources_cpu = task_resources_cpu
        self.task_resources_memory_mb = task_resources_memory_mb
        self.active_deadline_seconds = active_deadline_seconds
        self.retries = retries
        self.cleanup = cleanup

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
        cache_key = naming.cache_key_from_task_name(task_name)
        task_num = naming.task_num_from_task_name(task_name)
        logging.info(
            "Generating kubernetes specification for task %d in job %s" % (
                task_num, cache_key))

        sanitized_task_name = naming.sanitize(task_name)
        sanitized_cache_key = naming.sanitize(cache_key)

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
                        "name": str(task_num),
                        "image": self.image,
                        "imagePullPolicy": self.image_pull_policy,
                        "command": run_task_args(
                            task_input,
                            task_output,
                            cleanup=self.cleanup),
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
