"""YAML config loader with env-var interpolation.

Reads a YAML config file and substitutes ${VAR} placeholders with env vars.
Keeps secrets out of the repo while making local dev trivial.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

_ENV_PATTERN = re.compile(r"\$\{([A-Z0-9_]+)(?::-([^}]*))?\}")


def _substitute_env(value: Any) -> Any:
    """Recursively replace ${VAR} and ${VAR:-default} in strings."""
    if isinstance(value, str):

        def replace(match: re.Match) -> str:
            var_name = match.group(1)
            default = match.group(2) or ""
            return os.environ.get(var_name, default)

        return _ENV_PATTERN.sub(replace, value)
    if isinstance(value, dict):
        return {k: _substitute_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_substitute_env(v) for v in value]
    return value


def load_config(path: str | Path) -> dict[str, Any]:
    """Load YAML config with env-var interpolation.

    Raises:
        FileNotFoundError: config file missing.
        yaml.YAMLError: malformed YAML.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    return _substitute_env(raw)
