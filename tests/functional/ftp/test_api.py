import dataio


def test_authenticated_api_calls():
    data = bytes("Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn", "utf-8")

    with dataio.open("ftp://hostname/data.txt", mode="wb") as f:
        f.write(data)

    with dataio.open("ftp://hostname/data.txt", mode="rb") as f:
        result = f.read()

    assert result == data
