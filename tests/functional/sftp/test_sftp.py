import dataio


def test_with_creds():
    data = bytes("Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn", "utf-8")
    with dataio.open("sftp://octopus:tentacle@localhost/upload/data.txt", mode="wb") as f:
        f.write(data)

    with dataio.open("sftp://octopus:tentacle@localhost/upload/data.txt", mode="rb") as f:
        result = f.read()
    assert result == data
