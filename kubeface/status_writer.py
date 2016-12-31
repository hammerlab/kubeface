from io import BytesIO
import json
import time

from . import naming, storage


class DefaultStatusWriter(object):
    def __init__(self, storage_prefix, job_name):
        self.storage_prefix = storage_prefix
        self.job_name = job_name
        self.json_path = (
            storage_prefix +
            "/" +
            naming.status_page_name(job_name, "json", "active"))
        self.html_path = (
            storage_prefix +
            "/" +
            naming.status_page_name(job_name, "html", "active"))

    def print_info(self):
        print("Job status available at:")
        print("\t%s" % storage.access_info(self.json_path))
        print("\t%s" % storage.access_info(self.html_path))

    def make_html(self, status_dict):
        d = dict(status_dict)
        d["num_running_tasks"] = len(d["running_tasks"])
        d["num_completed_tasks"] = len(d["completed_tasks"])
        d["num_submitted_tasks"] = len(d["submitted_tasks"])
        d["num_reused_tasks"] = len(d["reused_tasks"])
        if d["num_tasks"]:
            d["percent_complete"] = (
                d["num_completed_tasks"] * 100.0 / d["num_tasks"])
        else:
            d["percent_complete"] = "unknown"
            d["num_tasks"] = "unknown"
        d["status_time"] = time.asctime()

        return """
        <html>
        <head>
        <title>Kubeface status: {job_name}</title>
        </head>
        <body>
        <h1>Kubeface</h1>
        <h2>{job_name}</h2>
        <table>
        <tr><td>Job</td><td>{job_name}</td></tr>
        <tr><td>Cache key</td><td>{cache_key}</td></tr>
        <tr><td>Backend</td><td>{backend}</td></tr>
        <tr><td>Max simultaneous tasks</td>
            <td>{max_simultaneous_tasks}</td></tr>
        <tr><td>Start time</td><td>{start_time}</td></tr>
        <tr><td>Status time</td><td>{status_time}</td></tr>
        </table>

        <h2>Status</h2>
        <table>
        <tr><td>Percent complete</td><td>{percent_complete}</td></tr>
        <tr><td>Running tasks</td><td>{num_running_tasks}</td></tr>
        <tr><td>Completed tasks</td><td>{num_completed_tasks}</td></tr>
        <tr><td>Submitted tasks</td><td>{num_submitted_tasks}</td></tr>
        <tr><td>Reused tasks</td><td>{num_reused_tasks}</td></tr>
        <tr><td>Total tasks</td><td>{num_tasks}</td></tr>
        </table>
        </body>
        </html>
        """.format(**d)

    def update(self, status_dict):
        storage.put(
            self.json_path,
            BytesIO(json.dumps(status_dict).encode()),
            mime_type="application/json")
        storage.put(
            self.html_path,
            BytesIO(self.make_html(status_dict).encode()),
            mime_type="text/html")
