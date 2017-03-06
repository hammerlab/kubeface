import logging
import tempfile
from contextlib import closing

from . import common, serialization, storage


class RemoteObject(object):
    def __init__(self, file_path, value):
        self.file_path = file_path
        self._value = value
        self.written = False
        self.loaded = True

    @property
    def value(self):
        """
        Value is lazy loaded when it is first accessed.
        """
        if not self.loaded:
            with closing(storage.get(self.file_path)) as fd:
                self._value = serialization.load(fd)
            self.loaded = True
        return self._value

    def __getstate__(self):
        """
        The first time the object is pickled, we write it to file_path.
        The pickled representation is just the path to the file.
        """
        if not self.written:
            assert self.loaded
            with tempfile.TemporaryFile(prefix="kubeface-object-") as fd:
                serialization.dump(self._value, fd)
                logging.info("Writing object (%s): %s" % (
                    common.human_readable_memory_size(fd.tell()),
                    self.file_path))
                fd.seek(0)
                storage.put(self.file_path, fd)
            self.written = True
        return {"file_path": self.file_path}

    def __setstate__(self, state):
        assert list(state) == ['file_path']
        self.file_path = state['file_path']
        self._value = None
        self.written = True
        self.loaded = False
