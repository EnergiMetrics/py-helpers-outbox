from datetime import UTC, datetime

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator


class OutboxMessage(BaseModel):
    """Immutable outbox message; created_at is not the observation timestamp."""

    model_config = ConfigDict(frozen=True)

    id: int = Field(gt=0, strict=True)
    destination: str = Field(strict=True)
    payload: bytes = Field(strict=True)
    created_at: AwareDatetime

    delivered_at: AwareDatetime | None = None

    @field_validator("created_at", "delivered_at")
    @classmethod
    def normalize_utc(cls, value: datetime | None) -> datetime | None:
        return value.astimezone(UTC) if value is not None else None
