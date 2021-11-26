"""GS Functionality tests."""
import os
from urllib import parse

import pytest

from tentaclio import clients

GS_TEST_URL = os.getenv("TENTACLIO__CONN__GS_TEST")


@pytest.fixture(scope="session")
def gs_url():
    """Get the GS URL that has been set in env."""
    assert GS_TEST_URL is not None, "Missing gs URL in environment variables"
    return GS_TEST_URL


@pytest.fixture(scope="session")
def gs_client(gs_url):
    """Initialise GS client."""
    with clients.GSClient(gs_url) as client:
        yield client


@pytest.fixture
def test_bucket(gs_url):
    """Return the test bucket."""
    u = parse.urlparse(gs_url)
    assert u.netloc is not None, "Missing bucket in url."
    return u.netloc


@pytest.fixture
def bucket_exists(gs_client, test_bucket):
    """Create a test bucket for the functional test."""
    bucket = gs_client.conn.bucket(test_bucket)
    if not bucket.exists():
        gs_client.conn.create_bucket(test_bucket, predefined_acl="project-private")
    yield gs_client

