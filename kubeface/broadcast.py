import logging
import tempfile
from contextlib import closing

from . import common, serialization, storage


class Broadcast(object):

    def __init__(self, file_path, value):
        self.file_path = file_path
        self.value = value
        self.written = False

    def __getstate__(self):
        """
        The first time the Broadcast is pickled, we write it to file_path.
        The pickled representation is just the path to the file.
        """
        if not self.written:
            with tempfile.TemporaryFile(prefix="kubeface-broadcast-") as fd:
                serialization.dump(self.value, fd)
                logging.info("Writing broadcast (%s): %s" % (
                    common.human_readable_memory_size(fd.tell()),
                    self.file_path))
                fd.seek(0)
                storage.put(self.file_path, fd)
            self.written = True
        return {"file_path": self.file_path}

    def __setstate__(self, state):
        assert list(state) == ['file_path']
        self.file_path = state['file_path']
        with closing(storage.get(state['file_path'])) as fd:
            self.value = serialization.load(fd)
        self.written = True
