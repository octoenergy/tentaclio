import pytest

import dataio


TEST_BUCKET = "dataio-bucket"


@pytest.fixture
def fixture_client(s3_client):
    s3_client.conn.create_bucket(Bucket=TEST_BUCKET)
    yield s3_client


def test_authenticated_api_calls(fixture_client):
    data = bytes("Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn", "utf-8")

    with dataio.open(f"s3://hostname/data.txt", mode="wb") as f:
        f.write(data)

    with dataio.open(f"s3://hostname/data.txt", mode="rb") as f:
        result = f.read()

    assert result == data
