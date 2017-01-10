import hashlib
import logging

class Broadcast(object):
    def __init__(self, storage_prefix, data):
        hash_path = '%s/%s' % (storage_prefix, self.compute_filename(data))
        logging.info('Writing broadcast data to %s', hash_path)
        with open(hash_path, 'w') as f:
            f.write(data)
        self.path = hash_path
        self.data = None

    @staticmethod
    def compute_filename(data):
        return hashlib.md5(data.encode('utf-8')).hexdigest()

    def value(self):
        if self.data == None:
            with open(self.path, 'r') as f:
                self.data = f.read()
        return self.data
