#!/usr/bin/env python

import pathlib
from setuptools import find_packages, setup

VERSION = "0.0.1"

REPO_ROOT = pathlib.Path(__file__).parent

with open(REPO_ROOT / "README.md", encoding="utf-8") as f:
    README = f.read()

REQUIREMENTS = [
    # Domain
    "pandas>=0.23.4,<0.24",
    # AWS S3
    "boto3>=1.9.81,<1.10",
    # Postgres
    "psycopg2-binary",
    "sqlalchemy>=1.1,<1.3",
    # SFTP
    "pysftp>=0.2.9,<0.3",
    # protocols typing
    "typing-extensions",
]


setup_args = dict(
    # Description
    name="dataio",
    version=VERSION,
    description="Single repository regrouping all IO connectors used within the data world",
    long_description=README,
    # Credentials
    author="Octopus Energy",
    author_email="tech@octoenergy.com",
    url="https://github.com/octoenergy/data-io",
    license="Proprietary",
    # Package data
    package_dir={"": "src"},
    packages=find_packages("src", exclude=["*tests*"]),
    include_package_data=False,
    # Dependencies
    install_requires=REQUIREMENTS,
)


if __name__ == "__main__":

    # Make install
    setup(**setup_args)
