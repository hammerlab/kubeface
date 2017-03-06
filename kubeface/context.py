"""
This module defines information that allows code to determine if it is running
on a master Kubeface node (node_type == "master") or as a task
(node_type == "task").

This dict defaults to indicating running on a master node, and is updated by
the run-task command with task-specific information.
"""

from .naming import hash_value

RUNTIME_CONTEXT = {
    "node_type": "master",
    "task_input_path": None,
    "task_result_path": None,
}


def node_id():
    if RUNTIME_CONTEXT["node_type"] == "master":
        return "node-master"
    return "node-%s" % (
        hash_value(
            RUNTIME_CONTEXT["task_result_path"]))
