import pytest

import dataio


TEST_BUCKET = "test-bucket"


@pytest.fixture
def fixture_client(s3_client):
    s3_client.conn.create_bucket(Bucket=TEST_BUCKET)
    yield s3_client
    s3_client.conn.delete_bucket(Bucket=TEST_BUCKET)


def test_authenticated_api_calls(fixture_client):
    data = bytes("Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn", "utf-8")

    with dataio.open(f"s3://{TEST_BUCKET}/data.txt", mode="wb") as f:
        f.write(data)

    with dataio.open(f"s3://{TEST_BUCKET}/data.txt", mode="rb") as f:
        result = f.read()

    assert result == data
