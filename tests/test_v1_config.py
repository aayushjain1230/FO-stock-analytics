import json

import pytest

from app.services.configuration import load_config


def test_missing_config_uses_defaults(tmp_path):
    config = load_config(tmp_path / "missing.json")
    assert config.benchmark == "SPY"
    assert config.interval == "1d"


def test_invalid_config_raises(tmp_path):
    path = tmp_path / "config.json"
    path.write_text("{bad", encoding="utf-8")
    with pytest.raises(ValueError):
        load_config(path)


def test_invalid_explanation_depth_raises(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"settings": {"explanation_depth": "Verbose"}}), encoding="utf-8")
    with pytest.raises(ValueError):
        load_config(path)
