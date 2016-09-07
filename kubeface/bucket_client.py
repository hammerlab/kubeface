import logging
from six import StringIO

from googleapiclient import discovery
from googleapiclient import http

from oauth2client.client import GoogleCredentials

# Some of this is copied from:
# https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/storage/api/crud_object.py


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


class BucketClient(object):
    def __init__(self, bucket):
        self.service = create_service()
        self.bucket = bucket

    def upload_object(self, name, input_handle, readers=[], owners=[]):
        # This is the request body as specified:
        # http://g.co/cloud/storage/docs/json_api/v1/objects/insert#request
        body = {
            'name': name,
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
        req = self.service.objects().insert(
            bucket=self.bucket,
            body=body,
            # You can also just set media_body=filename, but # for the sake of
            # demonstration, pass in the more generic file handle, which could
            # very well be a StringIO or similar.
            media_body=http.MediaIoBaseUpload(
                input_handle, 'application/octet-stream'))
        resp = req.execute()

        return resp

    def get_object(self, name, output_handle=None):
        if output_handle is None:
            output_handle = StringIO()

        # Use get_media instead of get to get the actual contents of the object
        req = self.service.objects().get_media(bucket=self.bucket, object=name)

        downloader = http.MediaIoBaseDownload(output_handle, req)

        done = False
        while done is False:
            (status, done) = downloader.next_chunk()
            logging.debug("Download {}%.".format(int(status.progress() * 100)))

        return output_handle

    def delete_object(self, filename):
        req = self.service.objects().delete(
            bucket=self.bucket, object=filename)
        return req.execute()

