from pathlib import Path

import pytest

from siklink.config import load_config


def test_minimal_config_uses_defaults(tmp_path: Path):
    path = tmp_path / "config.yaml"
    path.write_text("node_name: base\n")
    config = load_config(path)
    assert config.serial.baud == 57600
    assert config.protocol.max_payload == 256


def test_unknown_key_is_rejected(tmp_path: Path):
    path = tmp_path / "config.yaml"
    path.write_text("node_name: base\nserial:\n  typo: true\n")
    with pytest.raises(ValueError, match="unknown serial"):
        load_config(path)
