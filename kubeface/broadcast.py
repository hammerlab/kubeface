import logging
import tempfile

from . import common, naming, serialization, storage


_KEY = 'filename'

class Broadcast(object):

    def __init__(self, storage_prefix, cache_key_prefix, data):
        self.storage_prefix = storage_prefix
        self.cache_key_prefix = cache_key_prefix
        self.data = data

    def compute_filename(self):
        return naming.make_broadcast_name(self.cache_key_prefix)

    """
    Returns a dict with one key-value pair, where the value is the path to which the data was
    written.
    """
    def __getstate__(self):
        # load data into a local tempfile and write out to storage
        filename = '%s/%s' % (self.storage_prefix, self.compute_filename())
        with tempfile.TemporaryFile(prefix="broadcast-data-") as fd:
            serialization.dump(self.data, fd)
            size_string = common.human_readable_memory_size(fd.tell())
            logging.info("Writing broadcast data (%s) to path %s", size_string, filename)
            fd.seek(0)
            storage.put(filename, fd)

        return {_KEY: filename}

    def __setstate__(self, state):
        input_handle = storage.get(state[_KEY])
        self.data = serialization.load(input_handle)
