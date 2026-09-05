"""Instate configuration."""

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Config:
    """Application configuration."""

    database_url: str = field(
        default_factory=lambda: os.environ.get(
            "INSTATE_DATABASE_URL",
            "sqlite+aiosqlite:///instate.db",
        )
    )

    @property
    def is_postgres(self) -> bool:
        return self.database_url.startswith("postgresql")

    @property
    def is_sqlite(self) -> bool:
        return self.database_url.startswith("sqlite")
