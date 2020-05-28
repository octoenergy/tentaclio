"""GS Functionallity tests.

To use run the test use command:
```
env TENTACLIO__CONN__GS_TEST=gs://tentaclio-bucket/test.txt \
    env TENTACLIO__CONN__GS_TEST_PROJECT=<project that the bucket is in> \
    make functional-gs
```

You will need to have you gcloud set up. See docs for more information:
    https://googleapis.dev/python/google-api-core/latest/auth.html
"""
import os
from urllib import parse

import pytest

from tentaclio import clients

TEST_BUCKET = "tentaclio-bucket"

GS_TEST_URL = os.getenv("TENTACLIO__CONN__GS_TEST")
GS_TEST_PROJECT = os.getenv("TENTACLIO__CONN__GS_TEST_PROJECT")


@pytest.fixture(scope="session")
def gs_url():
    """Get the GS URL that has been set in env."""
    assert GS_TEST_URL is not None, "Missing gs URL in environment variables"
    return GS_TEST_URL


@pytest.fixture(scope="session")
def gs_project():
    """Get GS project for testing."""
    return GS_TEST_PROJECT


@pytest.fixture(scope="session")
def gs_client(gs_url):
    """Initialise GS client."""
    with clients.GSClient(gs_url, project=GS_TEST_PROJECT) as client:
        yield client


@pytest.fixture
def test_bucket(gs_url):
    """Return the test bucket."""
    u = parse.urlparse(gs_url)
    assert u.netloc is not None, "Missing bucket in url."
    return u.netloc


@pytest.fixture
def fixture_client(gs_client, test_bucket):
    """Create a test bucket for the functional test."""
    bucket = gs_client.conn.bucket(test_bucket)
    if not bucket.exists():
        gs_client.conn.create_bucket(test_bucket, predefined_acl="project-private")
    yield gs_client

