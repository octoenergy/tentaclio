#!/usr/bin/env python
# -*- coding: utf-8 -*
"""Setup script with custom verify command for CI/CD."""
import os
import re
import sys
from pathlib import Path

from setuptools import setup
from setuptools.command.install import install


class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version."""

    description = "verify that the git tag matches our version"

    def run(self):
        # Read version from pyproject.toml using simple regex (avoids tomllib for Python 3.9/3.10)
        pyproject_path = Path(__file__).parent / "pyproject.toml"
        content = pyproject_path.read_text()
        match = re.search(r'^version\s*=\s*["\']([^"\']+)["\']', content, re.MULTILINE)
        if not match:
            sys.exit("Could not find version in pyproject.toml")
        version = match.group(1)
        
        tag = os.getenv("CIRCLE_TAG")
        if tag != version:
            info = f"Git tag: {tag} does not match the version of this app: {version}"
            sys.exit(info)


if __name__ == "__main__":
    setup(cmdclass={"verify": VerifyVersionCommand})
