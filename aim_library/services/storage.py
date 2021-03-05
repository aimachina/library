# pylint: disable=no-name-in-module
# pylint: disable=import-error
# pylint: disable=no-member
import io
from logging import warning

from botocore.exceptions import ClientError

import boto3
from utils.configmanager import ConfigManager


class S3:
    __resource = None
    __client = None

    def __init__(self, config=None):
        config = config or ConfigManager.get_config_value("aws", "s3")
        self.__resource = self._get_bucket(config)
        self.__client = boto3.client("s3")

    def _get_bucket(self, config=None):
        if not self.__resource:
            config = config or ConfigManager.get_config_value("aws", "s3")
            self.__resource = boto3.resource("s3").Bucket(config["bucket_name"])
        return self.__resource

    def upload_file(self, file, key):
        bucket = self._get_bucket()
        if type(file) is str:
            bucket.upload_file(file, key)
        else:
            bucket.upload_fileobj(file, key)
        return key

    def upload_bytes(self, key, _bytes, extra_args={"ContentType": "binary/octet-stream"}):
        obj = io.BytesIO(_bytes)
        bucket = self._get_bucket()
        bucket.upload_fileobj(obj, key, ExtraArgs=extra_args)
        return key

    def download_file(self, key, filename):
        with open(filename, "wb") as f:
            self.download_obj(key, f)

    def download_obj(self, key, handle):
        bucket = self._get_bucket()
        bucket.download_fileobj(key, handle)

    def download_bytes(self, key):
        stream = io.BytesIO()
        self.download_obj(key, stream)
        return stream.getvalue()

    def list_files(self, limit=1000):
        bucket = self._get_bucket()
        return (o.key for o in bucket.objects.limit(count=limit))

    def list_files_in_dir(self, directory):
        bucket = self._get_bucket()
        return (o.key for o in bucket.objects.filter(Prefix=directory) if o.key != directory)

    def delete_one(self, key):
        return self.delete_many([key])

    def delete_many(self, keys):
        bucket = self._get_bucket()
        objects = {"Objects": [{"Key": k} for k in keys]}
        rsp = bucket.delete_objects(Delete=objects)
        deleted = [o["Key"] for o in rsp["Deleted"]]
        errors = (
            [{"name": o["Key"], "code": o["Code"], "message": o["Message"]} for o in rsp["Errors"]]
            if "Errors" in rsp
            else []
        )
        return deleted, errors

    def exists(self, key):
        bucket = self.__client
        config = ConfigManager.get_config_value("aws", "s3")

        try:  # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object
            bucket.head_object(Bucket=config["bucket_name"], Key=key)

        except ClientError as client_error:
            if client_error.response["Error"]["Code"] == "404":
                return False
            else:
                raise client_error

        return True

    def get_bucket(self):
        return self.__resource
