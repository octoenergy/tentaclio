import io

import pytest

from tentaclio import urls
from tentaclio.clients import exceptions, s3_client


@pytest.fixture()
def mocked_conn(mocker):
    with mocker.patch.object(s3_client.S3Client, "_connect", return_value=mocker.Mock()) as m:
        yield m


class TestS3Client:
    @pytest.mark.parametrize(
        "url,username,password,hostname,path",
        [
            ("s3://:@s3", None, None, None, ""),
            ("s3://public_key:private_key@s3", "public_key", "private_key", None, ""),
            ("s3://:@bucket", None, None, "bucket", ""),
            ("s3://:@bucket/prefix", None, None, "bucket", "prefix"),
        ],
    )
    def test_parsing_s3_url(self, url, username, password, hostname, path):
        client = s3_client.S3Client(url)

        assert client.aws_access_key_id == username
        assert client.aws_secret_access_key == password
        assert client.key_name == path
        assert client.bucket == hostname

    @pytest.mark.parametrize(
        "url,bucket,key",
        [("s3://:@s3", None, None), ("s3://:@s3", "bucket", None), ("s3://:@bucket", None, None)],
    )
    def test_get_invalid_path(self, url, bucket, key, mocked_conn):
        with s3_client.S3Client(url) as client:
            with pytest.raises(exceptions.S3Error):
                client.get(io.StringIO(), bucket_name=bucket, key_name=key)

    @pytest.mark.parametrize("url", [("s3:///"), ("s3://")])
    def test_get_buckets(self, url, mocked_conn):

        with s3_client.S3Client(url) as client:

            expected_buckets = ("mocked-0", "mocked-1")
            bucket_names = [{"Name": bucket} for bucket in expected_buckets]
            client.conn.list_buckets.return_value = {"Buckets": bucket_names}

            entries = client.scandir()

            expected_urls = set([str(urls.URL("s3://" + bucket)) for bucket in expected_buckets])
            result_urls = set([str(entry.url) for entry in entries])

            assert result_urls == expected_urls
            assert all([entry.is_dir for entry in entries])


class TestKeyLister(object):
    def test_files(self, mocker):
        client = mocker.MagicMock(bucket="bucket", key_name="deep/key")
        client.conn.get_paginator().paginate.return_value = [
            {"Contents": [{"Key": "deep/key/file0.txt"}, {"Key": "deep/key/file1.txt"}]}
        ]
        expected_urls = set(["s3://bucket/deep/key/file1.txt", "s3://bucket/deep/key/file0.txt"])

        lister = list(s3_client._KeyLister(client))
        urls = [str(entry.url) for entry in lister]

        assert set(urls) == expected_urls
        assert all([entry.is_file for entry in lister])

    def test_folders(self, mocker):
        client = mocker.MagicMock(bucket="bucket", key_name="deep/key")
        client.conn.get_paginator().paginate.return_value = [
            {"CommonPrefixes": [{"Prefix": "deep/key/folder0/"}, {"Prefix": "deep/key/folder1/"}]}
        ]
        expected_urls = set(["s3://bucket/deep/key/folder0", "s3://bucket/deep/key/folder1"])

        lister = s3_client._KeyLister(client)
        urls = [str(entry.url) for entry in lister]
        assert set(urls) == expected_urls
        assert all([entry.is_dir for entry in lister])

    def test_multiple_pages(self, mocker):
        client = mocker.MagicMock(bucket="bucket", key_name="deep/key")
        client.conn.get_paginator().paginate.return_value = [
            {
                "CommonPrefixes": [
                    {"Prefix": "deep/key/folder0/"},
                    {"Prefix": "deep/key/folder1/"},
                ],
                "Contents": [{"Key": "deep/key/file0.txt"}, {"Key": "deep/key/file1.txt"}],
            },
            {"Contents": [{"Key": "deep/key/file2.txt"}]},
        ]
        expected_urls = set(
            [
                "s3://bucket/deep/key/folder0",
                "s3://bucket/deep/key/folder1",
                "s3://bucket/deep/key/file0.txt",
                "s3://bucket/deep/key/file1.txt",
                "s3://bucket/deep/key/file2.txt",
            ]
        )

        lister = s3_client._KeyLister(client)
        urls = [str(entry.url) for entry in lister]
        assert set(urls) == expected_urls


@pytest.mark.parametrize(
    "url, expected",
    (
        ("s3://bucket", urls.URL("s3://bucket/last_path")),
    ),
)
def test_build_url(url, expected, mocker):
    client = s3_client.S3Client(url)

    url = s3_client._build_url(client.bucket, "last_path")
    assert url == expected

    url = s3_client._build_url(client.bucket, "last_path/")
    assert url == expected
