from dataio import api


def test_authenticated_api_calls():
    data = bytes("Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn", "utf-8")

    with api.open("ftp://localhost/data.txt", mode="wb") as f:
        f.write(data)

    with api.open("ftp://localhost/data.txt", mode="rb") as f:
        result = f.read()

    assert result == data
