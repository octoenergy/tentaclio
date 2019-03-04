import pickle

import pytest

import dataio


TEST_BUCKET = "dataio-bucket"


@pytest.fixture
def fixture_client(s3_client):
    s3_client.conn.create_bucket(Bucket=TEST_BUCKET)
    yield s3_client


def test_pickle(fixture_client):
    expected = """
    This is a highly convoluted test,
    with multiple output...
    encountered.
    """

    with dataio.open(f"s3://hostname/data.pickle", mode="wb") as f:
        pickle.dump(expected, f)

    with dataio.open(f"s3://hostname/data.pickle", mode="rb") as f:
        retrieved = pickle.load(f)

    assert expected == retrieved
