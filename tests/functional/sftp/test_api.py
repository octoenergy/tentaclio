import pytest

import tentaclio
import tentaclio.clients
from tentaclio.credentials import authenticate


def test_authenticated_api_calls():
    data = bytes("Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn", "utf-8")

    with tentaclio.open("sftp://hostname/upload/data.txt", mode="wb") as f:
        f.write(data)

    with tentaclio.open("sftp://hostname/upload/data.txt", mode="rb") as f:
        result = f.read()

    assert result == data


def test_list_folders():
    data = "Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn"
    cli = tentaclio.clients.SFTPClient(authenticate("sftp://hostname"))

    with cli:
        cli.makedirs("upload/folder1/folder2")

    with tentaclio.open("sftp://hostname/upload/folder1/data.txt", mode="w") as f:
        f.write(data)

    ls = list(tentaclio.listdir("sftp://hostname/upload/folder1"))
    assert any("upload/folder1/data.txt" in entry for entry in ls)
    assert any("upload/folder1/folder2" in entry for entry in ls)


def test_delete():
    source = "sftp://hostname/upload/source.txt"
    with tentaclio.open(source, mode="w") as f:
        f.write("Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn.")
    tentaclio.remove(source)
    with pytest.raises(tentaclio.clients.exceptions.FTPError):
        with tentaclio.open(source) as f:
            ...
