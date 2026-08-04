# Contributing to Cryptomeria

Thank you for your interest in contributing to Cryptomeria, a Medium-Frequency Trading (MFT) platform for crypto markets.

## How to Contribute

1. Fork the repository and create a feature branch from `main`.
2. Make your changes following the project conventions.
3. Run quality checks with `make check` (lint + test) or `make quick` (format + lint + test).
4. Submit a pull request with a clear description of the change and its motivation.

## Code Standards

- **Python**: Type hints mandatory (`str | None` union syntax), `@dataclass` for data containers, `pathlib.Path` for I/O, no mutable defaults, no bare `except Exception`.
- **Rust**: Edition 2024, `cargo clippy -D warnings` clean, no `catch (_)`.
- **No comments in code** — intent expressed via names; decisions documented in commits and ADRs.
- **Progress logging** — operations exceeding 10 seconds must emit progress every 5 seconds.
- **Secrets** in `.env.local` only (never committed).
- **Relative paths** only — no absolute paths in code, docs, or config.

## PR Workflow

- PR title should match the issue title.
- PR body should summarize changes and reference the issue.
- All tests must pass before review.

## Testing

- Every change requires unit tests and end-to-end tests.
- Python: `pytest` under `python/tests/`.
- Rust: `mod tests` for unit tests, `tests/` for ignored integration tests.
- Run all tests: `make test`.

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0 with additional brand protection terms (see [LICENSE](LICENSE)).
