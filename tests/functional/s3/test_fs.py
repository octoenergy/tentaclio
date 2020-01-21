import pytest

import tentaclio as tio
from tentaclio.clients.exceptions import S3Error


def test_scandir(fixture_client, test_bucket):
    files = ["file1.txt", "file2.txt", "file3.txt", "file4.txt"]
    folders = ["folder2", "folder3"]
    base = f"s3://{test_bucket}/folder1/"
    paths = []
    for file_ in files:
        paths.append(base + f"{file_}")

    for folder in folders:
        paths.append(base + f"{folder}/file_to_create_key.txt")

    for path in paths:
        with tio.open(path, mode="w") as f:
            f.write("Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn.")

    entries_by_url = {str(entry.url): entry for entry in tio.scandir(base)}

    for file_ in files:
        key = base + f"{file_}"
        assert key in entries_by_url
        assert entries_by_url[key].is_file

    for folder in folders:
        key = base + f"{folder}"
        assert key in entries_by_url
        assert entries_by_url[key].is_dir


def test_copy(fixture_client, test_bucket):
    source = f"s3://{test_bucket}/source.txt"
    dest = f"s3://{test_bucket}/dest.txt"
    with tio.open(source, mode="w") as f:
        f.write("Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn.")

    tio.copy(source, dest)

    with tio.open(dest) as f:
        result = f.read()

    assert result == "Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn."


def test_delete(fixture_client, test_bucket):
    source = f"s3://{test_bucket}/source.txt"
    with tio.open(source, mode="w") as f:
        f.write("Ph'nglui mglw'nafh Cthulhu R'lyeh wgah'nagl fhtagn.")
    tio.rm(source)
    with pytest.raises(S3Error, match="Unable to fetch the remote file"):
        with tio.open(source) as f:
            ...
