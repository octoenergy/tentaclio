SHELL := /bin/bash

.PHONY: all reset update clean sync test lint unit integration release package

all: reset test

# Local installation

reset: clean update

update:
	pipenv update --dev

clean:
	rm -rf build dist htmlcov
	find src -type d -name __pycache__ | xargs rm -rf
	pipenv clean

sync:
	pipenv sync --dev
	pipenv run pip install -e .

# Testing

test: lint unit

lint:
	pipenv run flake8 src
	pipenv run mypy src
	pipenv run pydocstyle src
	pipenv run flake8 tests
	pipenv run mypy tests

unit:
	unset TENTACLIO__PG_APPLICATION_NAME; pipenv run pytest tests/unit

functional-ftp:
	pipenv run pytest tests/functional/ftp

functional-sftp:
	pipenv run pytest tests/functional/sftp

format:
	pipenv run black -l 99 src
	pipenv run black -l 99 tests
	pipenv run isort src
	pipenv run isort tests

# Deployment

circleci:
	circleci config validate

release: package
	pipenv run twine upload dist/*

# Release
package:
	# create a source distribution
	pipenv run python setup.py sdist
	# create a wheel
	pipenv run python setup.py bdist_wheel
