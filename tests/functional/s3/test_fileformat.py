import pickle

import pytest

import tentaclio


TEST_BUCKET = "tentaclio-bucket"


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

    with tentaclio.open(f"s3://hostname/data.pickle", mode="wb") as f:
        pickle.dump(expected, f)

    with tentaclio.open(f"s3://hostname/data.pickle", mode="rb") as f:
        retrieved = pickle.load(f)

    assert expected == retrieved
