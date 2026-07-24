# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Tentaclio (`src/tentaclio/`) unifies stream I/O (`tentaclio.open`), db connections (`tentaclio.db`), and credential injection behind URL strings, so callers can swap `file://`, `ftp://`, `sftp://`, `s3://`, `postgresql://`, etc. without changing code. Extra schemes (s3, gs, gdrive, postgres, athena, databricks, snowflake) ship as separate `tentaclio_*` plugin packages, auto-discovered and imported at import time (`clients/importer.py::import_tentaclio_plugins`) via `importlib_metadata.packages_distributions()` — a plugin just needs to be pip-installed to register itself.

Package manager is `uv`. Python >=3.9, must support 3.9–3.12.

## Commands

```sh
make sync              # uv sync --dev
make test              # lint + unit
make lint              # flake8, mypy, pydocstyle on src AND tests
make unit              # pytest tests/unit (unsets TENTACLIO__PG_APPLICATION_NAME first)
make format            # black -l 99 + isort on src and tests
make functional-ftp     # pytest tests/functional/ftp (needs a running ftp server)
make functional-sftp    # pytest tests/functional/sftp (needs a running sftp server)
make docs-build          # build Sphinx HTML docs to docs/_build/html (needs `make install-docs-deps` first)
make docs                # docs-build, then serve docs/_build/html over http.server
```

Run a single test with uv directly, e.g.:
```sh
uv run pytest tests/unit/streams/test_api.py::test_open_read_mode -v
```

Coverage config (`pyproject.toml`) makes `pytest` always compute coverage over `src/tentaclio`; this is on by default via `addopts`, no extra flags needed.

Functional tests (`tests/functional/`) require live FTP/SFTP servers and are not run by `make test`/CI `unit` job — don't assume they pass without the servers up.

`postgres_url`/`db_client` fixtures in `tests/conftest.py` require `TENTACLIO__CONN__POSTGRES_TEST` to be set; tests using them will fail/skip without it.

## Architecture

Three parallel subsystems, each following the same registry pattern:

- **Streams** (`streams/`) — `tentaclio.open(url, mode)`. `streams/api.py` authenticates the URL then delegates to `STREAM_HANDLER_REGISTRY` (`streams/stream_registry.py`), which looks up a `StreamHandler` by URL scheme and wraps the result in `_ReaderContextManager`/`_WriterContextManager` (defined there, not in `base_stream.py`) so `.close()` always happens on context exit. Actual reader/writer classes live in `streams/base_stream.py`.
- **Databases** (`databases/`) — `tentaclio.db(url)`. `databases/api.py` authenticates then looks up a client factory in `DB_REGISTRY` (`databases/db_registry.py`) by scheme.
- **Filesystem ops** (`fs/`) — `listdir`/`scandir`/`walk`/`remove`/`copy` each have their own registry (`SCANNER_REGISTRY`, `REMOVER_REGISTRY`, copier logic in `fs/copier.py`/`fs/copiers.py`) keyed by scheme, same pattern.

All three registries are instances of the generic `URLHandlerRegistry` (`registry.py`): `register(scheme, handler)` / `get_handler(scheme)` (raises `KeyError` if unregistered). Registration happens at import time in `src/tentaclio/__init__.py` — e.g. `STREAM_HANDLER_REGISTRY.register("sftp", StreamURLHandler(SFTPClient))`. When adding a new scheme, register it there (or, for a plugin package, in that package's own import-time registration).

**Clients** (`clients/`) all subclass `BaseClient` (`clients/base_client.py`): constructor takes a `URL` or url string, validates `url.scheme` is in the class's `allowed_schemes`, and implements `_connect()` (abstract). `BaseClient` handles `__enter__`/`__exit__`/`close()` bookkeeping (`closed` flag) so subclasses only implement connection logic.

**URLs** (`urls.py`) — `URL` is immutable; wraps `urllib.parse`. `URL.from_components(...)` builds one from parts; `.copy(...)` returns a modified copy. `__str__` masks passwords and query keys like `private_key_path`/`private_key_password` — never bypass this when logging URLs.

**Credentials** (`credentials/`) — `authenticate(url)` (`credentials/api.py`) runs a URL through the process-wide `CredentialsInjector` singleton (built once via `load_credentials_injector()`, `lru_cache`d), sourced from env vars prefixed `TENTACLIO__CONN__` (`credentials/env.py`) and/or a YAML file pointed to by `TENTACLIO__SECRETS_FILE` (`credentials/reader.py`). Injection matching (`credentials/injection.py`) picks the best-matching stored credential for a given target URL by hostname, then path-segment similarity, then username — `hostname` in a URL is a wildcard meaning "match any host with stored creds for this scheme". This similarity logic is deliberately asymmetric (see docstring in `injection.py::_similarity`); don't "simplify" it without re-reading why.

**Protocols** (`protocols.py`) — structural typing (`typing.Protocol`) is used throughout instead of ABC subclassing, to decouple implementations from concrete dependencies (see README's "protocols structural subtyping" section and the linked tech blog post). When composing protocols (e.g. `ReaderClosable(Reader, Closable, Protocol)`), `Protocol` must be the last base class or the composition breaks.

## Conventions

- flake8 max-line-length / black / isort line-length is 99, not 79/88.
- `pydocstyle` is enforced on `src` — public modules/classes/functions need docstrings.
- mypy runs with `check_untyped_defs`, `warn_redundant_casts`, `warn_unused_ignores` on both `src` and `tests`.
- isort groups: STDLIB, THIRDPARTY, FIRSTPARTY (`tentaclio`), LOCALFOLDER, with a blank line after imports.
- `pytest` is configured to error on `RuntimeWarning` (`filterwarnings = ["error::RuntimeWarning"]`), so an unclosed resource or similar warning fails tests rather than just printing.
