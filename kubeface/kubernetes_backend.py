import tempfile
import json
import logging

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
        parser.add_argument(
            "--kubernetes-keep-input",
            dest="kubernetes_delete_input",
            action="store_false",
            default=True)

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
            active_deadline_seconds=args.kubernetes_active_deadline_seconds,
            delete_input=args.kubernetes_delete_input)

    def __init__(
            self,
            image,
            image_pull_policy="Always",
            cluster=None,
            task_resources_cpu=1,
            task_resources_memory_mb=1000,
            active_deadline_seconds=100,
            delete_input=True):
        self.image = image
        self.image_pull_policy = image_pull_policy
        self.cluster = cluster
        self.task_resources_cpu = task_resources_cpu
        self.task_resources_memory_mb = task_resources_memory_mb
        self.active_deadline_seconds = active_deadline_seconds
        self.delete_input = delete_input

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
            fd.flush()
            check_call(["kubectl", "create", "-f", fd.name])
        return task_name

    def task_specification(self, task_name, task_input, task_output):
        job_name = naming.job_name_from_task_name(task_name)
        task_num = naming.task_num_from_task_name(task_name)
        logging.info(
            "Generating kubernetes specification for task %d in job %s" % (
                task_num, job_name))

        sanitized_task_name = naming.sanitize(task_name)
        result = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": sanitized_task_name,
            },
            "spec": {
                "activeDeadlineSeconds": self.active_deadline_seconds,
                "template": {
                    "metadata": {
                        "name": str(task_num),
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
                                    delete_input=self.delete_input),
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
                        "restartPolicy": "OnFailure",
                    },
                },
            },
        }
        return result
