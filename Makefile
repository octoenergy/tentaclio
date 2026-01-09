SHELL := /bin/bash

.PHONY: all reset clean sync test lint unit integration release check-release package

all: reset test

# Local installation

reset: clean sync

clean:
	rm -rf build dist htmlcov
	find src -type d -name __pycache__ | xargs rm -rf
	uv clean

sync:
	uv sync --dev

# Testing

test: lint unit

lint:
	uv run flake8 src
	uv run mypy src
	uv run pydocstyle src
	uv run flake8 tests
	uv run mypy tests

unit:
	unset TENTACLIO__PG_APPLICATION_NAME; uv run pytest tests/unit

functional-ftp:
	uv run pytest tests/functional/ftp

functional-sftp:
	uv run pytest tests/functional/sftp

format:
	uv run black -l 99 src
	uv run black -l 99 tests
	uv run isort src
	uv run isort tests

# Deployment

circleci:
	circleci config validate

release: package
	uv run twine upload dist/*
check-release: package
	uv run twine check dist/*

# Release
package:
	# Build source distribution and wheel using modern build tools
	uv build

# Docs
install-docs-deps:
	uv sync --group docs

docs:
	uv run mkdocs serve

docs-build:
	cd docs && uv run --group docs make html
