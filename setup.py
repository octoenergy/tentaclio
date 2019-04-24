#!/usr/bin/env python
# -*- coding: utf-8 -*
import pathlib
from setuptools import find_packages, setup

VERSION = "0.0.1-alpha.1"

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
    # Athena
    "PyAthena[SQLAlchemy]",
    # SFTP
    "pysftp>=0.2.0,<0.3",
    # Utils
    "typing-extensions",
    "pandas",
    "click",
    "pyyaml",
]


setup_args = dict(
    # Description
    name="tentaclio",
    version=VERSION,
    description="Unification of data connectors for distributed data tasks",
    long_description=README,
    long_description_content_type='text/markdown',
    # Credentials
    author="Octopus Energy",
    author_email="nerds@octoenergy.com",
    url="https://github.com/octoenergy/tentaclio",
    license="MIT",
    # Package data
    package_dir={"": "src"},
    packages=find_packages("src", include=["*tentaclio*"]),
    entry_points={"console_scripts": ["tentaclio = tentaclio.cli:main"]},
    include_package_data=False,
    # Dependencies
    install_requires=REQUIREMENTS,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Typing :: Typed",
    ],
)


if __name__ == "__main__":

    # Make install
    setup(**setup_args)
