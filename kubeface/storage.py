import os
import glob
from . import bucket_storage


def is_google_storage_bucket(name):
    return name.startswith("gs://")


def list_contents(prefix):
    if is_google_storage_bucket(prefix):
        return bucket_storage.list_contents(prefix)
    else:
        globbed = glob.glob(prefix + "*")
        return [os.path.basename(x) for x in globbed]


def put(name, input_handle, readers=[], owners=[], **kwargs):
    if is_google_storage_bucket(name):
        return bucket_storage.put(
            name, input_handle, readers, owners, **kwargs)

    # Local file
    with open(name, 'wb') as fd:
        fd.write(input_handle.read())


def get(name, output_handle=None):
    if is_google_storage_bucket(name):
        return bucket_storage.get(name, output_handle)

    # Local file
    if output_handle is None:
        return open(name, "rb")

    with open(name, "rb") as fd:
        output_handle.write(fd.read())

    return output_handle


def delete(name):
    if is_google_storage_bucket(name):
        return bucket_storage.delete(name)

    os.unlink(name)


def move(source, dest):
    if is_google_storage_bucket(source):
        assert is_google_storage_bucket(dest)
        return bucket_storage.move(source, dest)
    assert not is_google_storage_bucket(dest)
    os.rename(source, dest)


def access_info(name):
    if is_google_storage_bucket(name):
        return bucket_storage.access_info(name)
    return name
