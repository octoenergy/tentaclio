SHELL := /bin/bash

.PHONY: all reset clean lock update sync test lint unit functional-ftp functional-sftp format \
        circleci package check-release release install-docs-deps docs docs-build

all: reset test


# Local installation

reset: clean sync

clean:
	rm -rf build dist htmlcov
	find src -type d -name __pycache__ | xargs rm -rf
	uv clean

lock: ## Lock dependencies
	uv lock

update: ## Update dependencies (whole tree)
	uv lock --upgrade

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


# Build source distribution and wheel using modern build tools

package:
	uv build


# Release

check-release: package
	uv publish --dry-run

release: package
	uv publish


# Docs
install-docs-deps:
	uv sync --group docs

docs: docs-build
	cd docs/_build/html && uv run python -m http.server

docs-build:
	cd docs && uv run --group docs make html
