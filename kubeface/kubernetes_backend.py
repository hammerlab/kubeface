import time
import logging
import getpass
import socket

from . import serialization
from .backend import Backend
#from .common import check_call


class KubernetesBackend(Backend):

    @staticmethod
    def add_args(parser):
        parser.add_argument("--kubernetes-image")
        parser.add_argument(
            "--kubernetes-image-pull-policy",
            default="Always")
        parser.add_argument("--kubernetes-cluster")

    @staticmethod
    def from_args(args):
        if not args.image:
            raise ValueError("--kubernetes-image is required")
        return KubernetesBackend(
            args.image,
            args.image_pull_policy,
            args.cluster)

    def __init__(
            self,
            image,
            image_pull_policy="Always",
            cluster=None):
        self.image = image
        self.image_pull_policy = image_pull_policy
        self.cluster = cluster

    def submit_task(self, task_input, task_output):
        raise NotImplemented

'''

class TimeoutError(object):
    pass


class KubernetesTaskFuture(Future):
    def __init__(self, backend, task_name):
        self.backend = backend
        self.task_name = task_name
        self.result_name = self.backend.task_result_name(task_name)

    def get_full_payload(self, timeout=None, poll_interval=10):
        logging.info("Polling for result: %s" % self.result_name)
        start = time.time()
        result_data = None
        while result_data is None:
            try:
                result_data = self.backend.bucket_client.get_object(
                    self.result_name)
            except NotImplementedError:  # change
                seconds_elapsed = time.time() - start
                if timeout is None or seconds_elapsed < timeout:
                    time.sleep(min(poll_interval, timeout - seconds_elapsed))
                else:
                    logging.info("Timeout for result: %s" % self.result_name)
                    raise TimeoutError()
        logging.info("Downloaded result: %s" % self.result_name)
        return serialization.load(result_data)

    def get(self, **kwargs):
        return self.get_full_payload(**kwargs)['result']


def make_run_name():
    run_name = "%s-%s-%s" % (
        socket.gethostbyname(),
        getpass.getuser(),
        datetime.strftime(datetime.now(), "%Y-%m-%d-%H:%M:%S"),
        hashlib.sha1(str(time.time()).encode()).hexdigest()[:8])
    return run_name





class KubernetesBackend(Backend):
    def __init__(
            self,
            bucket_client,
            image,
            image_pull_policy="Always",
            shell_preamble=None,
            python_package_paths=[],
            cpu_request=1,
            memory_request=None,
            cluster=None,
            dry_run=False):

        # Lazy initialized
        self.run_name = None
        self.secret = None

        self.bucket_client = bucket_client
        self.image = image
        self.image_pull_policy = image_pull_policy
        self.cpu_request = cpu_request
        self.memory_request = memory_request
        self.cluster = cluster

    def initialize_run(self):
        assert self.run_name is None, "Already initialized"
        logging.info("Initializing run: %s" % self.run_name)
        self.run_name = make_run_name()
        self.tmp_dir = NamedTempDir

        self.setup_secrets()

    def setup_secrets(self):
        if self.python_package_paths:
            for package in self.python_package_paths:
                check_call("""
                    mkdir -p {self.tmp_dir}/secret_files &&
                    mkdir -p dist &&
                    mv dist dist.backup.{self.run_name} &&
                    python setup.py sdist --formats gztar &&
                    mv dist/* {self.tmp_dir}/secret_files &&
                    rm -rf dist &&
                    mv dist.backup.{self.run_name} dist
                """.replace("\n", " "), shell=True, cwd=package)

            self.secret = "%s-secret" % self.run_name
            check_call(
                "kubectl create secret generic {self.secret} "
                "--from-file={self.tmp_dir}/secret_files",
                shell=True)

    def make_command(self):
        command_pieces = []
        if self.shell_preamble:
            command_pieces.append(self.shell_preamble)
        if self.secret:
            command_pieces.append(
                "pip install /tmp/kubeface-secret-files/*.tar.gz")

        command_pieces.append("mkdir /tmp/kubeface-worker")
        command_pieces.append("cd /tmp/kubeface-worker")
        command_pieces.append(
            "kubeface-copy $KUBEFACE_WORKER_INPUT input.pkl")
        command_pieces.append(
            "kubeface-copy --no-error $KUBEFACE_WORKER_OUTPUT output.pkl")
        command_pieces.append(
            "if [ ! -e $KUBEFACE_WORKER_OUTPUT ] ; "
            "then kubeface-worker /tmp/kubeface-worker/input.pkl output.pkl "
            " && kubeface-copy output.pkl $KUBEFACE_WORKER_OUTPUT "
            "; else ; echo 'Work already done.' ; fi ")

        return [
            "/bin/bash",
            "-c",
            " && ".join(command_pieces),
        ]


    def task_specification(self, task:
        result = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": task.name,
            },
            "spec": {
                "template": {
                    "metadata": {
                        "name": task.name,
                    },
                    "spec": {
                        "containers": [
                            {
                                "name": task.name,
                                "image": self.image,
                                "imagePullPolicy": self.image_pull_policy,
                                "command": [],
                                "volumeMounts": ([{
                                    "name": "%s-secret" % self.run_name,
                                    "mountPath": "/etc/kubeface-secret-files",
                                    "readOnly": true
                                }] if self.secret else [])
                            }
                        ],
                        "restartPolicy": "OnFailure",
                        "volumes": ([{
                            "name": "%s-secret" % self.run_name,
                            "secret": {
                                "secretName": "%s-secret" % self.run_name,
                            },
                        }] if self.secret else []),
                    },
                },
            },
        }
        return result

    def task_result_name(self, task_name):
        return "result-%s" % task_name

    def submit(self, function, args, kwargs):
        task_name = "task-%s-%s" % (
            self.run_name,
            hashlib.sha1(serialized).hexdigest())

        task = Task(task_name, function, args, kwargs)





        command = [
            "kubectl",
            "run",
            task_name,
            "--image", self.image,
            "--cluster", self.cluster,
            "--command", "true",
        ]
        request_parts = []
        if self.cpu_request is not None:
            request_parts.append("cpu=%s" % self.cpu_request)
        if self.memory_request is not None:
            request_parts.append("memory=%s" % self.memory_request)
        if request_parts:
            command.extend("--requests", ",".join(request_parts))

        if self.dry_run:
            command.append("--dry-run")

        command.append("--")
        command.extend(invocation)
        logging.info("Running: %s")
        subprocess.check_call(command)
'''