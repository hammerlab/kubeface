import subprocess
import logging


class Backend(object):
    def schedule(self):
        raise NotImplementedError


class Future(object):
    def get(self, timeout=None):
        raise NotImplementedError
