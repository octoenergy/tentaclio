from tentaclio import URL
from tentaclio.fs.scanner import build_file_entry, build_folder_entry


def test_build_folder_entry():
    url = URL("scantest://myurl")
    entry = build_folder_entry(url)
    assert entry.url == url
    assert entry.is_dir
    assert not entry.is_file


def test_build_file_entry():
    url = URL("scantest://myurl")
    entry = build_file_entry(url)
    assert entry.url == url
    assert entry.is_file
    assert not entry.is_dir
