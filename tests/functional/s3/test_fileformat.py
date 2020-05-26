import pickle


import tentaclio


def test_pickle(fixture_client):
    expected = """
    This is a highly convoluted test,
    with multiple output...
    encountered.
    """

    with tentaclio.open("s3://hostname/data.pickle", mode="wb") as f:
        pickle.dump(expected, f)

    with tentaclio.open("s3://hostname/data.pickle", mode="rb") as f:
        retrieved = pickle.load(f)

    assert expected == retrieved
