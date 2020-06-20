import io

import boto3
from utils.configmanager import ConfigManager


class S3:
    __bucket = None

    def __init__(self, config=None):
        config = config or ConfigManager.get_config_value("aws", "s3")
        self.__bucket = self._get_bucket(config)

    def _get_bucket(self, config=None):
        if not self.__bucket:
            config = config or ConfigManager.get_config_value("aws", "s3")
            self.__bucket = boto3.resource("s3").Bucket(config["bucket_name"])
        return self.__bucket

    def upload_file(self, file, key):
        bucket = self._get_bucket()
        if type(file) is str:
            bucket.upload_file(file, key)
        else:
            bucket.upload_fileobj(file, key)
        return key

    def upload_bytes(self, _bytes, key):
        obj = io.BytesIO(_bytes)
        bucket = self._get_bucket()
        bucket.upload_fileobj(obj, key)
        return key

    def download_file(self, key, filename):
        with open(filename, "wb") as f:
            self.download_obj(key, f)

    def download_obj(self, key, handle):
        bucket = self._get_bucket()
        bucket.download_fileobj(key, handle)

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

    def get_bucket(self):
        return self.__bucket
