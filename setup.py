#!/usr/bin/env python

import pathlib
from setuptools import find_packages, setup

VERSION = "0.0.1"

REPO_ROOT = pathlib.Path(__file__).parent

with open(REPO_ROOT / "README.md", encoding="utf-8") as f:
    README = f.read()

REQUIREMENTS = [
    # AWS
    "boto3>=1.9.0,<1.10",
    # Http
    "requests",
    # Postgres
    "psycopg2-binary",
    "sqlalchemy>=1.2.0,<1.3",
    # SFTP
    "pysftp>=0.2.0,<0.3",
    # Utils
    "typing-extensions",
    "pandas",
    "click",
    "pyyaml"
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
    packages=find_packages("src", include=["*dataio*"]),
    entry_points={
        'console_scripts': [
            'data-url = dataio.cli.url:command_compose_url'
        ]
    },
    include_package_data=False,
    # Dependencies
    install_requires=REQUIREMENTS,
)


if __name__ == "__main__":

    # Make install
    setup(**setup_args)
