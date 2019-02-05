from dataio import open_reader, open_writer


def test_with_creds():
    data = bytes("Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn", "utf-8")
    with open_writer("sftp://octopus:tentacle@localhost:22/upload/data.txt", mode="b") as f:
        f.write(data)

    with open_reader("sftp://octopus:tentacle@localhost:22/upload/data.txt", mode="b") as f:
        result = f.read()
    assert result == data
