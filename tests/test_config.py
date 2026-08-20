from pathlib import Path

import pytest

from siklink.config import load_config


def test_minimal_config_uses_defaults(tmp_path: Path):
    path = tmp_path / "config.yaml"
    path.write_text("node_name: base\n")
    config = load_config(path)
    assert config.serial.baud == 57600
    assert config.protocol.max_payload == 256
    assert config.serial.usb_reset_after_reopens == 1
    assert config.protocol.heartbeat_reply_timeout == 2.0


def test_unknown_key_is_rejected(tmp_path: Path):
    path = tmp_path / "config.yaml"
    path.write_text("node_name: base\nserial:\n  typo: true\n")
    with pytest.raises(ValueError, match="unknown serial"):
        load_config(path)


def test_rf_silence_reopen_must_not_precede_disconnect(tmp_path: Path):
    path = tmp_path / "config.yaml"
    path.write_text(
        "node_name: base\nserial:\n  rf_silence_reopen_after: 2\n"
        "protocol:\n  disconnect_after: 3\n"
    )
    with pytest.raises(ValueError, match="rf_silence_reopen_after"):
        load_config(path)


def test_heartbeat_reply_timeout_must_exceed_interval(tmp_path: Path):
    path = tmp_path / "config.yaml"
    path.write_text(
        "node_name: base\nprotocol:\n  heartbeat_interval: 1\n"
        "  heartbeat_reply_timeout: 1\n"
    )
    with pytest.raises(ValueError, match="timing/count"):
        load_config(path)
