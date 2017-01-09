class Backend(object):
    def submit_task(self, task_input, task_output):
        raise NotImplementedError

    def supports_storage_prefix(self, storage_prefix):
    	raise NotImplementedError
