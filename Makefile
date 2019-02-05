SHELL := /bin/bash

.PHONY: all reset update clean sync test lint unit integration

all: reset test

# Local installation

reset: clean update

update:
	pipenv update --dev
	pipenv clean

clean:
	rm -rf build dist
	find src -type d -name __pycache__ | xargs rm -rf

sync:
	pipenv sync --dev
	pipenv clean

# Testing

test: lint unit integration

lint:
	pipenv run flake8 src
	pipenv run mypy src
	pipenv run flake8 tests
	pipenv run mypy tests

format:
	black -l 99 --py36 src
	black -l 99 --py36 tests
	isort -l 99 -m 3 -lai 2 -rc src
	isort -l 99 -m 3 -lai 2 -rc tests

unit:
	pipenv run pytest tests/unit

integration:
	pipenv run pytest tests/integration

coverage:
	pipenv run pytest --cov=src --cov-report html --cov-report term tests

functional-ftp:
	pipenv run pytest tests/functional/ftp

functional-sftp:
	pipenv run pytest tests/functional/sftp

# Deployment

package:
	pipenv run python setup.py bdist_wheel

circleci:
	circleci config validate
