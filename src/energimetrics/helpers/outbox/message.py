from datetime import UTC, datetime

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator


class OutboxMessage(BaseModel):
    """Immutable pending message; created_at is not the observation timestamp."""

    model_config = ConfigDict(frozen=True)

    id: int = Field(gt=0, strict=True)
    destination: str = Field(strict=True)
    payload: bytes = Field(strict=True)
    created_at: AwareDatetime

    @field_validator("created_at")
    @classmethod
    def normalize_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)
