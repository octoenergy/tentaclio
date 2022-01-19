#!/usr/bin/env python
# -*- coding: utf-8 -*
import os
import pathlib
import sys

from setuptools import find_packages, setup
from setuptools.command.install import install


VERSION = "1.0.3"

REPO_ROOT = pathlib.Path(__file__).parent

with open(REPO_ROOT / "README.md", encoding="utf-8") as f:
    README = f.read()


class VerifyVersionCommand(install):
    """Custom command to verify that the git tag matches our version."""

    description = "verify that the git tag matches our version"

    def run(self):
        tag = os.getenv("CIRCLE_TAG")

        if tag != VERSION:
            info = f"Git tag: {tag} does not match the version of this app: {VERSION}"
            sys.exit(info)


REQUIREMENTS = [
    # Security constraints
    "urllib3>=1.24.2",
    # Http
    "requests",
    # Sqlalchemy
    # Pinned due to incompatibility with base client
    "sqlalchemy<1.4",
    # SFTP
    "pysftp>=0.2.0,<0.3",
    # Utils
    # Pinned to due requiring sqlalchemy>=1.4
    "pandas<1.4",
    "click",
    "pyyaml",
    "importlib_metadata",
]

PLUGINS = {
    "s3": "tentaclio_s3",
    "athena": "tentaclio_athena",
    "postgres": "tentaclio_postgres",
    "databricks": "tentaclio_databricks>=1.0.0",
    "gdrive": "tentaclio_gdrive",
    "gs": "tentaclio_gs",
}


setup_args = dict(
    # Description
    name="tentaclio",
    version=VERSION,
    description="Unification of data connectors for distributed data tasks",
    long_description=README,
    long_description_content_type="text/markdown",
    # Credentials
    author="Octopus Energy",
    author_email="nerds@octoenergy.com",
    url="https://github.com/octoenergy/tentaclio",
    license="MIT",
    # Package data
    package_dir={"": "src"},
    packages=find_packages("src", include=["*tentaclio*"]),
    include_package_data=False,
    # Dependencies
    install_requires=REQUIREMENTS,
    extras_require=PLUGINS,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Typing :: Typed",
    ],
    cmdclass={"verify": VerifyVersionCommand},
)


if __name__ == "__main__":

    # Make install
    setup(**setup_args)
