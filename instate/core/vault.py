"""Secrets vault interface (§15)."""

import os
from typing import Protocol


class Vault(Protocol):
    def get(self, key: str) -> str | None: ...
    def set(self, key: str, value: str) -> None: ...
    def rotate(self, key: str, new_value: str) -> None: ...


class EnvVault:
    """Env-backed vault: in-memory overrides, then <KEY>_FILE, then env."""

    def __init__(self):
        self._overrides: dict[str, str] = {}

    def get(self, key: str) -> str | None:
        if key in self._overrides:
            return self._overrides[key]
        file_var = os.environ.get(f"{key}_FILE")
        if file_var:
            try:
                with open(file_var, encoding="utf-8") as f:
                    value = f.read().strip()
                if value:
                    return value
            except OSError:
                pass  # unreadable file falls through to env
        return os.environ.get(key)

    def set(self, key: str, value: str) -> None:
        self._overrides[key] = value
        os.environ[key] = value

    def rotate(self, key: str, new_value: str) -> None:
        self.set(key, new_value)


class ExternalVault:
    """Stub for an external secret store."""

    def __init__(self, client):
        self._client = client

    def get(self, key: str) -> str | None:
        return self._client.read(key)

    def set(self, key: str, value: str) -> None:
        self._client.write(key, value)

    def rotate(self, key: str, new_value: str) -> None:
        self._client.rotate(key, new_value)


vault = EnvVault()
