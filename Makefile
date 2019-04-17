SHELL := /bin/bash

.PHONY: all reset update clean sync test lint unit integration

all: reset test

# Local installation

reset: clean update

update:
	pipenv update --dev
	pipenv clean

clean:
	rm -rf build dist htmlcov
	find src -type d -name __pycache__ | xargs rm -rf

sync:
	pipenv sync --dev
	pipenv clean

# Testing

test: lint unit

lint:
	pipenv run flake8 src
	pipenv run mypy src
	pipenv run pydocstyle src
	pipenv run flake8 tests
	pipenv run mypy tests

unit:
	pipenv run pytest tests/unit

functional-s3:
	pipenv run pytest tests/functional/s3

functional-ftp:
	pipenv run pytest tests/functional/ftp

functional-sftp:
	pipenv run pytest tests/functional/sftp

functional-postgres:
	pipenv run pytest tests/functional/postgres

format:
	black -l 99 src
	black -l 99 tests
	isort -rc src
	isort -rc tests

coverage:
	pipenv run pytest --cov=src --cov-report html --cov-report term tests

# Deployment

package:
	pipenv run python setup.py bdist_wheel

circleci:
	circleci config validate
