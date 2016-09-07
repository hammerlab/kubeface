import subprocess
import logging

class Backend(object):
    def schedule(self):
        raise NotImplementedError


class KubernetesBackend(Backend):
    def __init__(
            self,
            image,
            cpu_request=1,
            memory_request=None,
            cluster=None,
            dry_run=False):
        self.image = image
        self.cpu_request = cpu_request
        self.memory_request = memory_request
        self.cluster = cluster

    def schedule(self, task_name, invocation):
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
        