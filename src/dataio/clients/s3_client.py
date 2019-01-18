import boto3
from boto3.resources import base

from . import base_client, types


__all__ = ["S3Client"]


class S3Client(base_client.BaseClient):
    """
    Generic S3 hook, backed by boto3
    """

    def __init__(self, url: types.NoneString) -> None:
        # TODO: consider cases with AWS credentials as
        # 1/ environment variables
        # 2/ a config file with a specific profile
        super().__init__(url)

    # Connection methods:

    def get_conn(self) -> base.ServiceResource:
        return boto3.resource(
            "s3", aws_access_key_id=self.url.username, aws_secret_access_key=self.url.password
        )

    # Document methods:

    def get(self, **params) -> types.T:
        raise NotImplementedError

    def put(self, **params) -> None:
        raise NotImplementedError
