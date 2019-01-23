import boto3
import moto
import pytest

from dataio.buffers import s3_buffer


AWS_PUBLIC_KEY = "public_key"
AWS_PRIVATE_KEY = "private_key"


@pytest.fixture()
def fixture_conn():
    with moto.mock_s3():
        yield


class TestS3Buffer:
    @pytest.mark.parametrize(
        "url,bucket,key",
        [(f"s3://{AWS_PUBLIC_KEY}:{AWS_PRIVATE_KEY}@s3", "test-bucket", "test.key")],
    )
    def test_reading_key(self, url, bucket, key, fixture_conn):
        stream = "tested stream"
        conn = boto3.session.Session(
            aws_access_key_id=AWS_PUBLIC_KEY, aws_secret_access_key=AWS_PRIVATE_KEY
        ).client("s3")
        conn.create_bucket(Bucket=bucket)
        conn.put_object(Bucket=bucket, Key=key, Body=stream)

        with s3_buffer.open_s3_reader(url, bucket_name=bucket, key_name=key) as reader:
            assert reader.read().decode() == stream

    @pytest.mark.parametrize(
        "url,bucket,key",
        [(f"s3://{AWS_PUBLIC_KEY}:{AWS_PRIVATE_KEY}@s3", "test-bucket", "test.key")],
    )
    def test_writing_key(self, url, bucket, key, fixture_conn):
        stream = "tested stream"
        conn = boto3.session.Session(
            aws_access_key_id=AWS_PUBLIC_KEY, aws_secret_access_key=AWS_PRIVATE_KEY
        ).client("s3")
        conn.create_bucket(Bucket=bucket)

        with s3_buffer.open_s3_writer(url, bucket_name=bucket, key_name=key) as writer:
            writer.write(stream.encode())

        s3_obj = conn.get_object(Bucket=bucket, Key=key)["Body"]
        assert s3_obj.read().decode() == stream
