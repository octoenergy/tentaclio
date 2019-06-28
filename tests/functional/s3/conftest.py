"""Pytest config for s3 functional tests."""
import os

import pytest


TEST_BUCKET = "tentaclio-bucket"

S3_TEST_URL = os.getenv("TENTACLIO__CONN__S3_TEST")


@pytest.fixture(scope="session")
def s3_url():
    assert S3_TEST_URL is not None, "Missing s3 URL in environment variables"
    return S3_TEST_URL


@pytest.fixture
def test_bucket():
    """Return the test bucket."""
    return TEST_BUCKET


@pytest.fixture
def fixture_client(s3_client, test_bucket):
    """Create a test bucket for the functional test."""
    s3_client.conn.create_bucket(Bucket=TEST_BUCKET)
    yield s3_client
