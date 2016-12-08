import tempfile
import json

from .backend import Backend
from .local_process_backend import run_task_args
from .common import check_call


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
            "--kubernetes-keep-input",
            dest="kubernetes_delete_input",
            action="store_false",
            default="true")

    @staticmethod
    def from_args(args):
        if not args.image:
            raise ValueError("--kubernetes-image is required")
        return KubernetesBackend(
            image=args.image,
            image_pull_policy=args.image_pull_policy,
            cluster=args.cluster,
            task_resources_cpu=args.kubernetes_task_resources_cpu,
            task_resources_memory=args.kubernetes_task_resources_memory_mb,
            delete_input=args.kubernetes_delete_input)

    def __init__(
            self,
            image,
            image_pull_policy="Always",
            cluster=None,
            task_resources_cpu=1,
            task_resources_memory_mb=1000,
            delete_input=True):
        self.image = image
        self.image_pull_policy = image_pull_policy
        self.cluster = cluster
        self.task_resources_cpu = task_resources_cpu
        self.task_resources_memory_mb = task_resources_memory_mb
        self.delete_input = delete_input

    def submit_task(self, task_name, task_input, task_output):
        specification = self.task_specification(
            task_name,
            task_input,
            task_output)
        with tempfile.NamedTemporaryFile(
                prefix="kubeface-kubernetes-%s" % task_name,
                suffix=".json") as fd:
            json.dump(specification, fd, indent=4)
            fd.flush()
            check_call(["kubectl", "create", "-f", fd.name])
        return task_name

    def task_specification(self, task_name, task_input, task_output):
        result = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": task_name,
            },
            "spec": {
                "template": {
                    "metadata": {
                        "name": task_name,
                    },
                    "spec": {
                        "containers": [
                            {
                                "name": task_name,
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
