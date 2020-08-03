#!/usr/bin/env python
# -*- coding: utf-8 -*
import os
import pathlib
import sys

from setuptools import find_packages, setup
from setuptools.command.install import install


VERSION = "0.0.5"

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
    # Security constrains
    "urllib3>=1.24.2",
    # AWS
    "boto3>=1.9.0,<1.10",
    # GS
    "google-cloud-storage",
    # Http
    "requests",
    # Postgres
    "psycopg2-binary",
    # Sqlalchemy
    "sqlalchemy>1.3",
    # Athena
    "PyAthena",
    # SFTP
    "pysftp>=0.2.0,<0.3",
    # Google drive
    "google-api-python-client",
    "google-auth-httplib2",
    "google-auth-oauthlib",
    # Utils
    "typing-extensions",
    "pandas<1.0.2",
    "click",
    "pyyaml",
]


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
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Typing :: Typed",
    ],
    cmdclass={"verify": VerifyVersionCommand},
)


if __name__ == "__main__":

    # Make install
    setup(**setup_args)
