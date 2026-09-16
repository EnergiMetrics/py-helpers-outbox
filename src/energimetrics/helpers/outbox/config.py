from pathlib import Path

from pydantic import BaseModel, ConfigDict, field_validator


class OutboxConfig(BaseModel):
    """Location of a persistent SQLite database file."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    path: Path

    @field_validator("path")
    @classmethod
    def validate_path(cls, path: Path) -> Path:
        if path == Path(".") or str(path) == ":memory:" or "\x00" in str(path):
            raise ValueError("path must identify a persistent database file")
        return path
