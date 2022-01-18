"""Method for importing tentaclio plugins."""
import importlib
import logging
from typing import Callable, Iterable

from importlib_metadata import packages_distributions


PackageLister = Callable[[], Iterable[str]]

logger = logging.getLogger(__name__)


def import_tentaclio_plugins(
    list_packages: PackageLister = packages_distributions().keys,
) -> None:
    """Find and import tentaclio plugins."""
    for package in list_packages():
        if package.startswith("tentaclio_"):
            logger.info(f"Importing plugin: {package}")
            importlib.import_module(package)
