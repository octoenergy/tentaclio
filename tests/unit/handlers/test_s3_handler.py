# from dataio.handlers import s3_handler
import boto3
from dataio.urls import URL, open_reader, open_writer

import moto
import pytest

AWS_PUBLIC_KEY = "public_key"
AWS_PRIVATE_KEY = "private_key"


@pytest.fixture()
def fixture_conn():
    with moto.mock_s3():
        yield


def test_s3_is_registered():
    # this would break if s3 is not registered
    url = URL("s3://my_bucket/my_file")
    assert url.scheme == "s3"


def test_open_s3_url_writing(fixture_conn):
    conn = boto3.session.Session(
        aws_access_key_id=AWS_PUBLIC_KEY, aws_secret_access_key=AWS_PRIVATE_KEY
    ).client("s3")
    conn.create_bucket(Bucket="my_bucket")

    expected = bytes("hello from url", "utf-8")
    with open_writer("s3://public_key:private_key@my_bucket/my_file") as writer:
        writer.write(expected)

    with open_reader("s3://public_key:private_key@my_bucket/my_file") as reader:
        contents = reader.read()

    assert contents == expected
