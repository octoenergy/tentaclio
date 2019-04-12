"""Credentials injection utilities."""
import collections
import itertools
import logging
import re
from typing import Dict, List

from tentaclio import urls


logger = logging.getLogger(__name__)

__all__ = ["CredentialsInjector"]

HOSTNAME_WILDCARD = "hostname"


class CredentialsInjector(object):
    """Allows registering and injecting credentials to urls."""

    def __init__(self):
        """Create a new credentials injector."""
        self.registry: Dict[str, List[urls.URL]] = collections.defaultdict(list)

    def register_credentials(self, url: urls.URL) -> None:
        """Register a set of credentials."""
        self.registry[url.scheme].append(url)

    def inject(self, url: urls.URL) -> urls.URL:
        """Inject credentials to the given url if they are available."""
        creds_by_scheme = self.registry[url.scheme]
        creds_by_host = _filter_by_hostname(url, creds_by_scheme)
        candidates = _filter_by_path(url, creds_by_host)

        if len(candidates) == 0:
            return url
        else:
            if len(candidates) > 1:
                logger.warning(
                    "The credentials for %s returned %s candidates" % (str(url), len(candidates))
                )

            # return the best candidate, i.e the one with the highest similarity to
            # the passed url
            creds = candidates[0]
            with_creds = urls.URL.from_components(
                scheme=url.scheme,
                username=creds.username,
                password=creds.password,
                hostname=creds.hostname,
                port=creds.port or url.port,
                path=url.path,
                query=url.query or creds.query,
            )
            return with_creds


_URLSimilarity = collections.namedtuple("_URLSimilarity", ["url", "similarity"])


def _filter_by_path(url: urls.URL, to_match: List[urls.URL]) -> List[urls.URL]:
    # compute the similarties between the path of the url and the list to match
    similarities = map(lambda cred: _similarity(url.path, cred.path), to_match)
    url_similarities = itertools.starmap(_URLSimilarity, zip(to_match, similarities))
    # filter out the urls without any similarity
    positives = filter(lambda url_sim: url_sim.similarity > 0, url_similarities)
    # order them by similarity the similarity the better
    positives_ordered = sorted(positives, key=lambda url_sim: url_sim.similarity, reverse=True)
    return list(map(lambda url_sim: url_sim.url, positives_ordered))


def _filter_by_hostname(url: urls.URL, to_match: List[urls.URL]) -> List[urls.URL]:
    if url.hostname != "hostname":
        return list(filter(lambda cred: cred.hostname == url.hostname, to_match))
    return to_match


PATH_DELIMITERS = "/|::"


def _similarity(path_1: str, path_2: str) -> float:
    """Compute the similarity of two strings.

    The similarity is calculated as the
    number of path elements (separated by / or ::) that they have in common.
    "/path/to/mything" and "/path/to/mything" have 3 as similarity index.
    "/path" and "/path/to/mything" have 1 as similarity index.
    "" and "" have similarity index 0.5
    "/mypath" and "" have 0.5 similarity index.

    Similarity is not symetrical:
    "" and "/mypath" have similarity 0.
    This is because
    url: "ftp://hostname/file.txt" creds: "ftp://user@hostname/" should inject to
    "ftp://user@hostname/file.txt"

    but:

    url: "db://hostname/" creds: "db://user@hostname/mydatabase" should not inject to
    "db://user@hostname/database".

    That means that the path of the url to inject to should be more specific than the credentials.
    """
    path_1 = _clean_path(path_1)
    path_2 = _clean_path(path_2)

    if not path_1 and not path_2:
        return 0.5  # non-zero similarity when path is empty

    if not path_1 or not path_2:
        return 0.5

    # split path elements

    path_1_elements = re.split(PATH_DELIMITERS, path_1)
    path_2_elements = re.split(PATH_DELIMITERS, path_2)

    return _compute_parts_similarity(path_1_elements, path_2_elements)


def _clean_path(path: str):
    path = path or ""
    return path.strip("/")


def _compute_parts_similarity(elements_1: List[str], elements_2: List[str]) -> float:
    similarity = 0
    for idx, path_element in enumerate(elements_1):
        if idx == len(elements_2):
            break
        if path_element == elements_2[idx]:
            similarity += 1
        else:
            break
    return similarity
