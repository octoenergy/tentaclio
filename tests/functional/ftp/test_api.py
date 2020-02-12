import pytest

import tentaclio
from tentaclio.credentials import authenticate


def test_authenticated_api_calls():
    data = bytes("Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn", "utf-8")

    with tentaclio.open("ftp://hostname/data.txt", mode="wb") as f:
        f.write(data)

    with tentaclio.open("ftp://hostname/data.txt", mode="rb") as f:
        result = f.read()

    assert result == data


def cdTree(ftp, currentDir):
    # https://stackoverflow.com/a/18342179
    if currentDir != "":
        try:
            ftp.cwd(currentDir)
        except Exception:
            cdTree(ftp, "/".join(currentDir.split("/")[:-1]))
            ftp.mkd(currentDir)
            ftp.cwd(currentDir)


def test_list_folders():
    data = "Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn"
    cli = tentaclio.clients.FTPClient(authenticate("ftp://hostname"))

    with cli:
        cdTree(cli.conn, "/upload/folder1/folder2")

    with tentaclio.open("ftp://hostname/upload/folder1/data.txt", mode="w") as f:
        f.write(data)

    ls = list(tentaclio.listdir("ftp://hostname/upload/folder1"))
    assert any("upload/folder1/data.txt" in entry for entry in ls)
    assert any("upload/folder1/folder2" in entry for entry in ls)


def test_delete():
    source = f"ftp://hostname/source.txt"
    with tentaclio.open(source, mode="w") as f:
        f.write("Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn.")
    tentaclio.remove(source)
    with pytest.raises(tentaclio.clients.exceptions.FTPError):
        with tentaclio.open(source) as f:
            ...
