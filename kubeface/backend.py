class Backend(object):
    def submit_task(self, task_input, task_output):
        raise NotImplementedError

    def supports_storage(self, path_or_url):
        return True
