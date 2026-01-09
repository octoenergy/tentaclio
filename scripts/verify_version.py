#!/usr/bin/env python3
"""Verify that the git tag matches the version in pyproject.toml."""
import os
import re
import sys
from pathlib import Path


def get_version_from_pyproject():
    """Read version from pyproject.toml."""
    pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
    content = pyproject_path.read_text()
    match = re.search(r'^version\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
    if not match:
        print("ERROR: Could not find version in pyproject.toml")
        sys.exit(1)
    return match.group(1)


def main():
    """Verify git tag matches version."""
    version = get_version_from_pyproject()
    tag = os.getenv("CIRCLE_TAG")

    if not tag:
        print("ERROR: CIRCLE_TAG environment variable not set")
        sys.exit(1)

    if tag != version:
        print(f"ERROR: Git tag: {tag} does not match the version: {version}")
        sys.exit(1)

    print(f"✓ Git tag {tag} matches version in pyproject.toml")


if __name__ == "__main__":
    main()
