import pytest
import boto3


from services.storage import S3
from utils.configmanager import ConfigManager

TEST_CONFIG = "/config/receipts/test_config.yml"
FIXTURES_PATH = "/receipts/src/tests/library/services/data/"
PREFIX = "storage-tests/"
filename_1 = "small.jpg"
s3_config = ConfigManager.get_config_value("aws", "s3", TEST_CONFIG)


def setup_module(module):
    clean_all_files(s3_config, PREFIX)


def teardown_module(module):
    pass
    # clean_all_files(s3_config, PREFIX)


def clean_all_files(config, prefix):
    print(
        f'\nDeleting all files in bucket: \'{config["bucket_name"]}\' with prefix: \'{prefix}\''
    )
    bucket = boto3.resource("s3").Bucket(config["bucket_name"])
    bucket.objects.filter(Prefix=prefix).delete()


@pytest.fixture
def s3():
    s3 = S3(s3_config)
    yield s3


def test_upload_file(s3):
    s3.upload_file(FIXTURES_PATH + filename_1, PREFIX + filename_1)


def test_upload_bytes(s3):
    with open(FIXTURES_PATH + filename_1, "r+b") as f:
        data = f.read()
    s3.upload_bytes(data, PREFIX + "frombytes/" + filename_1)


def test_list_uploaded_files_in_previous_tests(s3):
    files = s3.list_files()
    assert type(files) is list and len(files) == 2


def test_should_filter_one_file(s3):
    files = s3.list_files_in_dir(PREFIX + "frombytes/")
    assert type(files) is list and len(files) == 1


def test_delete_one_file(s3):
    deleted, errors = s3.delete_one(PREFIX + "frombytes/" + filename_1)
    assert len(errors) == 0


def test_delete_all(s3):
    files = s3.list_files()
    deleted, errors = s3.delete_many(files)
