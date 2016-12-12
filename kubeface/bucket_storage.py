import logging
import tempfile
import time

from googleapiclient import discovery
from googleapiclient import http

from oauth2client.client import GoogleCredentials

from googleapiclient.errors import HttpError

# Some of this is copied from:
# https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/storage/api/crud_object.py
# and:
# https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/storage/api/list_objects.py


def create_service():
    # Get the application default credentials. When running locally, these are
    # available after running `gcloud init`. When running on compute
    # engine, these are available from the environment.
    credentials = GoogleCredentials.get_application_default()

    # Construct the service object for interacting with the Cloud Storage API -
    # the 'storage' service, at version 'v1'.
    # You can browse other available api services and versions here:
    #     http://g.co/dev/api-client-library/python/apis/
    return discovery.build('storage', 'v1', credentials=credentials)


SERVICE = create_service()

RETRIES_BEFORE_FAILURE = 12
FIRST_RETRY_SLEEP = 2.0


def robustify(function):
    def robust_function(*args, **kwargs):
        error_num = 0
        while True:
            try:
                return function(*args, **kwargs)
            except HttpError as e:
                error_num += 1
                logging.warning(
                    "Google API error calling %s: '%s'. "
                    "This call has failed %d times. Will retry up to "
                    "%d times." % (
                        str(function),
                        str(e),
                        error_num,
                        RETRIES_BEFORE_FAILURE))

                if error_num > RETRIES_BEFORE_FAILURE:
                    raise

                sleep_time = FIRST_RETRY_SLEEP**error_num
                logging.warn("Sleeping for %0.2f seconds." % sleep_time)
                time.sleep(sleep_time)
    return robust_function


def split_bucket_and_name(url):
    if not url.startswith("gs://"):
        raise ValueError("Not a gs:// url: %s" % url)
    return url[len("gs://"):].split("/", 1)


@robustify
def list_contents(prefix):
    splitted = split_bucket_and_name(prefix)
    if len(splitted) == 1:
        (bucket_name, file_name_prefix) = (splitted[0], "")
    else:
        (bucket_name, file_name_prefix) = splitted

    # Create a request to objects.list to retrieve a list of objects.
    fields_to_return = \
        'nextPageToken,items(name)'
    req = SERVICE.objects().list(
        bucket=bucket_name,
        prefix=file_name_prefix,
        fields=fields_to_return)

    all_objects = []
    # If you have too many items to list in one request, list_next() will
    # automatically handle paging with the pageToken.
    while req:
        resp = req.execute()
        all_objects.extend(resp.get('items', []))
        req = SERVICE.objects().list_next(req, resp)
    return [item['name'] for item in all_objects]


@robustify
def put(name, input_handle, readers=[], owners=[]):
    (bucket_name, file_name) = split_bucket_and_name(name)

    # This is the request body as specified:
    # http://g.co/cloud/storage/docs/json_api/v1/objects/insert#request
    body = {
        'name': file_name,
    }

    # If specified, create the access control objects and add them to the
    # request body
    if readers or owners:
        body['acl'] = []

    for r in readers:
        body['acl'].append({
            'entity': 'user-%s' % r,
            'role': 'READER',
            'email': r
        })
    for o in owners:
        body['acl'].append({
            'entity': 'user-%s' % o,
            'role': 'OWNER',
            'email': o
        })

    # Now insert them into the specified bucket as a media insertion.
    req = SERVICE.objects().insert(
        bucket=bucket_name,
        body=body,
        # You can also just set media_body=filename, but # for the sake of
        # demonstration, pass in the more generic file handle, which could
        # very well be a StringIO or similar.
        media_body=http.MediaIoBaseUpload(
            input_handle, 'application/octet-stream'))
    resp = req.execute()

    return resp


@robustify
def get(name, output_handle=None):
    (bucket_name, file_name) = split_bucket_and_name(name)

    if output_handle is None:
        output_handle = tempfile.TemporaryFile(
            prefix="kubeface-bucket-storage-",
            suffix=".data")

    # Use get_media instead of get to get the actual contents of the object
    req = SERVICE.objects().get_media(bucket=bucket_name, object=file_name)
    downloader = http.MediaIoBaseDownload(output_handle, req)

    done = False
    while done is False:
        (status, done) = downloader.next_chunk()
        logging.debug("Download {}%.".format(int(status.progress() * 100)))
    output_handle.seek(0)
    return output_handle


@robustify
def delete(name):
    (bucket_name, file_name) = split_bucket_and_name(name)
    req = SERVICE.objects().delete(bucket=bucket_name, object=file_name)
    return req.execute()
