"""Unit tests for utility modules."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from src.utils.config_loader import load_config

pytestmark = pytest.mark.unit


def test_load_config_basic(tmp_path: Path):
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(
        yaml.safe_dump({"environment": "test", "paths": {"landing": "/tmp/raw"}})
    )
    cfg = load_config(cfg_file)
    assert cfg["environment"] == "test"
    assert cfg["paths"]["landing"] == "/tmp/raw"


def test_load_config_substitutes_env_vars(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("LANDING_BUCKET", "s3://my-bucket")
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(
        "environment: prod\npaths:\n  landing: ${LANDING_BUCKET}/events\n"
    )
    cfg = load_config(cfg_file)
    assert cfg["paths"]["landing"] == "s3://my-bucket/events"


def test_load_config_env_var_default(tmp_path: Path):
    # Ensure the var is NOT set
    os.environ.pop("DOES_NOT_EXIST", None)
    cfg_file = tmp_path / "cfg.yaml"
    cfg_file.write_text(
        "environment: prod\npaths:\n  landing: ${DOES_NOT_EXIST:-default-bucket}/raw\n"
    )
    cfg = load_config(cfg_file)
    assert cfg["paths"]["landing"] == "default-bucket/raw"


def test_load_config_missing_file(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "nope.yaml")
