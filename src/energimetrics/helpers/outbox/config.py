from datetime import timedelta
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, field_validator


class OutboxConfig(BaseModel):
    """Location of a persistent SQLite database file."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    path: Path
    delivered_retention: timedelta = Field(default=timedelta(hours=24), gt=timedelta(0))
    cleanup_interval: timedelta = Field(default=timedelta(hours=1), gt=timedelta(0))

    @field_validator("path")
    @classmethod
    def validate_path(cls, path: Path) -> Path:
        if path == Path(".") or str(path) == ":memory:" or "\x00" in str(path):
            raise ValueError("path must identify a persistent database file")
        return path
