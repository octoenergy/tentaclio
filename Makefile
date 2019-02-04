SHELL := /bin/bash

.PHONY: all reset update clean sync circleci lint test package

all: install server

# Local installation

reset: clean sync

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

circleci: lint unit

# check-untyped-defs is not a valid config item for setup.cfg
lint:
	pipenv run flake8 src
	pipenv run mypy src --check-untyped-defs
	pipenv run flake8 tests
	pipenv run mypy tests --check-untyped-defs

unit:
	pipenv run pytest tests/unit

functional-postgres:
	pipenv run pytest tests/functional/postgres

functional-ftp:
	pipenv run pytest tests/functional/ftp

functional-sftp:
	pipenv run pytest tests/functional/ftp

# Deployment

package:
	pipenv run python setup.py bdist_wheel
