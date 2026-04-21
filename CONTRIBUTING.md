# Contributing

Thanks for your interest in contributing. This project follows standard data-engineering conventions.

## Code of Conduct

Be respectful. Assume good intent. Disagree on code, not people.

## Before you start

1. Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) to understand the design decisions
2. Read [docs/LOCAL_DEVELOPMENT.md](docs/LOCAL_DEVELOPMENT.md) to get a local dev environment running
3. Open an issue first for any substantial change (>50 lines) so we can align on approach

## Development workflow

```bash
# 1. Fork + clone
git clone https://github.com/YOUR_USERNAME/lakehouse-iot-telemetry-pipeline.git

# 2. Branch (use conventional prefix)
git checkout -b feat/descriptive-name   # or fix/, docs/, chore/, perf/, refactor/

# 3. Make changes + run local checks
make format
make lint
make typecheck
make test

# 4. Commit
git commit -m "feat: add retention check to silver layer"

# 5. Push + open PR
git push origin feat/descriptive-name
```

## PR requirements

- [ ] All CI checks green (lint, typecheck, unit tests, integration tests, terraform validate)
- [ ] New code covered by tests (unit AND integration if it touches Spark transforms)
- [ ] Updated docs if behavior changed
- [ ] Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)
- [ ] No secrets in diff (detect-secrets runs in CI)
- [ ] Terraform changes include `terraform plan` output in the PR description

## Code style

- Formatting: `ruff format` (configured in `pyproject.toml`)
- Linting: `ruff check` with the rule set in `pyproject.toml`
- Type hints: required on all public functions; enforced by `mypy`
- Docstrings: Google style; required on all public modules/classes/functions
- No `print()`; use `structlog` via `get_logger(__name__)`

## Spark-specific conventions

- **Never** use Python UDFs unless there's no native alternative — they disable Catalyst and Photon
- **Always** use native column expressions (`F.when`, `F.col`, etc.)
- **Never** call `.collect()` or `.toPandas()` without a prior `.limit()` or heavy filter
- **Always** partition Delta tables by date + tenant_id where applicable
- **Always** use MERGE for upserts on keyed tables, not append + dedupe
- **Never** hardcode paths; drive them from config YAML

## Security

- Never commit real credentials. Use `.env` (gitignored) or secrets manager
- New IAM policies must be least-privilege; PR reviewers will reject overly broad actions
- Flag any new PII column in the PR description so we can add UC tags

## Questions?

Open a [discussion](https://github.com/sushmakl95/lakehouse-iot-telemetry-pipeline/discussions) or ping me on [LinkedIn](https://www.linkedin.com/in/sushmakl1995/).
