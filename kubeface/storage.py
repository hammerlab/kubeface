import os

from . import bucket_storage


def is_google_storage_bucket(name):
    return name.startswith("gs://")


def put(name, input_handle, readers=[], owners=[]):
    if is_google_storage_bucket(name):
        return bucket_storage.put(name, input_handle, readers, owners)

    # Local file
    with open(name, 'w') as fd:
        input_handle.write(fd)


def get(name, output_handle=None):
    if is_google_storage_bucket(name):
        return bucket_storage.get(name, output_handle)

    # Local file
    if output_handle is None:
        return open(name)

    with open(name) as fd:
        output_handle.write(fd.read())

    return output_handle


def delete(name):
    if is_google_storage_bucket(name):
        return bucket_storage.delete(name)

    os.unlink(name)
