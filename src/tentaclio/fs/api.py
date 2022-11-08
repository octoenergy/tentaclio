"""Main entry points for fs/os like operations."""
from typing import Iterable, List, Tuple

from tentaclio import credentials

from .copier import COPIER_REGISTRY
from .remover import REMOVER_REGISTRY
from .scanner import SCANNER_REGISTRY, DirEntry


__all__ = ["scandir", "listdir", "copy", "remove", "walk"]


def scandir(url: str) -> Iterable[DirEntry]:
    """Scan a directory-like url returning its entries.

    Return an iterator of tentaclio.fs.DirEntry objects corresponding
    to the entries in the directory given by url. The entries
    are yielded in arbitrary order,
    and the special entries '.' and '..' are not included.

    More info:
    https://docs.python.org/3/library/os.html#os.scandir
    """
    authenticated = credentials.authenticate(url)
    return SCANNER_REGISTRY.get_handler(authenticated.scheme).scandir(authenticated)


def listdir(url: str) -> Iterable[str]:
    """List a directory-like url returning its entries.

    Return an iterator string containing the urls
    to the entries in the directory given by url passed as parametre. The entries
    are yielded in arbitrary order,
    and the special entries '.' and '..' are not included.

    More info:
    https://docs.python.org/3/library/os.html#os.listdir
    """
    entries = scandir(url)
    return (str(entry.url) for entry in entries)


def copy(source: str, dest: str):
    """Copy the contents of the origin URL into the dest url.

    Depending on the type of resource this can be much more performant than
    >>> with tio.open(source) as reader, tio.open(dest) as writer:
            writer.write(reader.read())
    """
    source_auth = credentials.authenticate(source)
    dest_auth = credentials.authenticate(dest)
    copier = COPIER_REGISTRY.get_handler(source_auth.scheme + "+" + dest_auth.scheme)
    copier.copy(source_auth, dest_auth)


def remove(url: str):
    """Delete the resource identified by the url."""
    authenticated = credentials.authenticate(url)
    REMOVER_REGISTRY.get_handler(authenticated.scheme).remove(authenticated)


def walk(top: str) -> Iterable[Tuple[str, str, str]]:
    """Generate the file names in a directory tree by walking the tree.

    For each directory in the tree rooted at directory top (including top itself),
    it yields a 3-tuple (dirpath, dirnames, filenames).
    """
    return _walk(top, [])


def _relativize(base: str, current: str) -> str:
    """Remove the base url from the current url."""
    if current.startswith(base):
        return current.replace(base, "", 1)
    return current


def _process_entry(
    entry: DirEntry, relative_path: str, dirs: List[str], files: List[str], to_walk: List[str]
):
    url = str(entry.url)
    if entry.is_dir:
        dirs.append(relative_path)
        to_walk.append(url)  # if a dir keep walking it.
    else:
        files.append(relative_path)
    return dirs, files, to_walk


def _walk(top: str, walked: list) -> List[Tuple[str, str, str]]:
    entries = scandir(top)
    to_walk: List[str] = []  # keep a list of dirs to walk
    dirs: List[str] = []
    files: List[str] = []

    for entry in entries:
        relative = _relativize(top, str(entry.url))
        dirs, files, to_walk = _process_entry(entry, relative, dirs, files, to_walk)

    walked.append((top, dirs, files))

    if len(to_walk):
        for new_top in to_walk:
            walked = _walk(new_top, walked)
    return walked
