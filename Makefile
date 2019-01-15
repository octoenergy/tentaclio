SHELL := /bin/bash

.PHONY: all reset update clean sync circleci lint type test package

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

circleci: lint type test

lint:
	pipenv run flake8 src

type:
	pipenv run mypy src

test:
	pipenv run pytest tests

# Deployment

package:
	pipenv run python setup.py bdist_wheel
