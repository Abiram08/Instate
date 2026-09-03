"""Secrets vault — env today, Vault/Secrets Manager tomorrow (§15).

The interface is the same either way: `vault.get("RAZORPAY_KEY")`.
In production, swap `EnvVault` for `ExternalVault` backed by
HashiCorp Vault / AWS Secrets Manager — rotation is a single call.
"""

import os
from typing import Protocol


class Vault(Protocol):
    def get(self, key: str) -> str | None: ...
    def set(self, key: str, value: str) -> None: ...
    def rotate(self, key: str, new_value: str) -> None: ...


class EnvVault:
    """Dev/demo vault: in-memory overrides, secret FILES, then env vars.

    Lookup order per key:
      1. in-memory override (set/rotate this process)
      2. `<KEY>_FILE` — path to a file holding the secret (Docker-secrets
         style). Secrets on disk are invisible to `ps aux`, unlike env.
      3. plain env var (convenient, least safe — fine for test keys)

    Rotation updates the override map and the process env so the next
    `get` sees it immediately.
    """

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


# Production hook — implement against your secret store:
class ExternalVault:
    """Stub for HashiCorp Vault / AWS Secrets Manager.

    Replace `get`/`rotate` with real SDK calls. The rest of the codebase
    never knows or cares.
    """

    def __init__(self, client):
        self._client = client

    def get(self, key: str) -> str | None:
        return self._client.read(key)

    def set(self, key: str, value: str) -> None:
        self._client.write(key, value)

    def rotate(self, key: str, new_value: str) -> None:
        self._client.rotate(key, new_value)


vault = EnvVault()
