"""Vault secrets with rotation and file reads."""

import os

from instate.core.vault import EnvVault


def test_env_lookup():
    os.environ["INSTATE_TEST_VAULT_KEY"] = "env-value"
    try:
        v = EnvVault()
        assert v.get("INSTATE_TEST_VAULT_KEY") == "env-value"
    finally:
        del os.environ["INSTATE_TEST_VAULT_KEY"]


def test_override_beats_env():
    os.environ["INSTATE_TEST_VAULT_KEY2"] = "env-value"
    try:
        v = EnvVault()
        v.set("INSTATE_TEST_VAULT_KEY2", "override-value")
        assert v.get("INSTATE_TEST_VAULT_KEY2") == "override-value"
    finally:
        del os.environ["INSTATE_TEST_VAULT_KEY2"]


def test_rotate_updates_immediately():
    v = EnvVault()
    v.set("INSTATE_TEST_ROT", "old")
    v.rotate("INSTATE_TEST_ROT", "new")
    assert v.get("INSTATE_TEST_ROT") == "new"
    assert os.environ["INSTATE_TEST_ROT"] == "new"
    del os.environ["INSTATE_TEST_ROT"]


def test_file_backed_secret_beats_env(tmp_path):
    secret_file = tmp_path / "razorpay_key"
    secret_file.write_text("file-secret-value\n")
    os.environ["INSTATE_TEST_FILE_KEY"] = "env-value"
    os.environ["INSTATE_TEST_FILE_KEY_FILE"] = str(secret_file)
    try:
        v = EnvVault()
        assert v.get("INSTATE_TEST_FILE_KEY") == "file-secret-value"
    finally:
        del os.environ["INSTATE_TEST_FILE_KEY"]
        del os.environ["INSTATE_TEST_FILE_KEY_FILE"]


def test_unreadable_file_falls_through_to_env(tmp_path):
    os.environ["INSTATE_TEST_MISSING_KEY"] = "env-fallback"
    os.environ["INSTATE_TEST_MISSING_KEY_FILE"] = str(tmp_path / "does-not-exist")
    try:
        v = EnvVault()
        assert v.get("INSTATE_TEST_MISSING_KEY") == "env-fallback"
    finally:
        del os.environ["INSTATE_TEST_MISSING_KEY"]
        del os.environ["INSTATE_TEST_MISSING_KEY_FILE"]


def test_missing_key_returns_none():
    v = EnvVault()
    assert v.get("INSTATE_DEFINITELY_NOT_SET_12345") is None
