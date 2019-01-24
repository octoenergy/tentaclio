SHELL := /bin/bash

.PHONY: all reset update clean sync circleci lint test package

all: install server

# Local installation

reset: clean install

update:
	pipenv update --dev
	pipenv clean

clean:
	-rm -rf build dist
	-find src -type d -name __pycache__ -delete

sync:
	pipenv sync --dev
	pipenv clean

# Testing

circleci: lint unit integration

# check-untyped-defs is not a valid config item for setup.cfg
lint:
	pipenv run flake8 src
	pipenv run mypy src --check-untyped-defs
	pipenv run flake8 tests
	pipenv run mypy tests --check-untyped-defs

unit:
	pipenv run pytest tests/unit

integration:
	pipenv run pytest tests/integration

# Deployment

package:
	pipenv run python setup.py bdist_wheel
