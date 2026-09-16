import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
from threading import RLock
from types import TracebackType
from typing import Self

from pydantic import ValidationError

from .config import OutboxConfig
from .message import OutboxMessage


class OutboxError(Exception):
    """The outbox could not complete an operation safely."""


class Outbox:
    """A thread-safe local outbox in an existing deployment-owned directory.

    The caller owns delivery and acknowledgement. SQLite creates the database,
    but its parent directory must already exist on persistent storage.
    """

    def __init__(self, config: OutboxConfig) -> None:
        self._lock = RLock()
        self._connection: sqlite3.Connection | None = None
        connection: sqlite3.Connection | None = None
        try:
            path = config.path.absolute()
            connection = sqlite3.connect(
                path,
                check_same_thread=False,
                # sqlite3 supports this sentinel; the type stub only allows bool.
                autocommit=sqlite3.LEGACY_TRANSACTION_CONTROL,  # pyright: ignore[reportArgumentType]
                isolation_level="IMMEDIATE",
            )
            connection.row_factory = sqlite3.Row
            mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            if mode != "wal":
                raise OutboxError("SQLite could not enable WAL durability")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA fullfsync=ON")
            if connection.execute("PRAGMA synchronous").fetchone()[0] != 2:
                raise OutboxError("SQLite could not enable FULL synchronization")
            with connection:
                connection.execute(
                    "CREATE TABLE IF NOT EXISTS outbox_messages ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "destination TEXT NOT NULL, "
                    "payload BLOB NOT NULL, "
                    "created_at TEXT NOT NULL)"
                )
        except (OSError, sqlite3.Error, OutboxError) as exc:
            if connection is not None:
                connection.close()
            raise OutboxError("Could not open durable outbox") from exc
        self._connection = connection

    @contextmanager
    def _operation(self) -> Generator[sqlite3.Connection]:
        with self._lock:
            if self._connection is None:
                raise OutboxError("Outbox is closed")
            try:
                # Commits writes before returning; rolls back on failure.
                with self._connection:
                    yield self._connection
            except (sqlite3.Error, ValidationError) as exc:
                raise OutboxError("Outbox database operation failed") from exc

    def enqueue(self, destination: str, payload: bytes | str) -> OutboxMessage:
        """Return only after commit. A failure may have an uncertain outcome."""
        # Validate callers from untyped application code as well.
        if not isinstance(destination, str):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError("destination must be str")
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        elif not isinstance(payload, bytes):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise TypeError("payload must be bytes or str")
        with self._operation() as connection:
            row = connection.execute(
                "INSERT INTO outbox_messages (destination, payload, created_at) "
                "VALUES (?, ?, ?) RETURNING id, destination, payload, created_at",
                (destination, payload, datetime.now(UTC).isoformat()),
            ).fetchall()[0]
            message = OutboxMessage.model_validate(dict(row))
        return message

    def pending(self, limit: int = 100) -> list[OutboxMessage]:
        """Read an oldest-first snapshot, without reserving messages for delivery."""
        self._validate_integer(limit, "limit")
        with self._operation() as connection:
            rows = connection.execute(
                "SELECT id, destination, payload, created_at "
                "FROM outbox_messages ORDER BY id LIMIT ?",
                (limit,),
            ).fetchall()
            return [OutboxMessage.model_validate(dict(row)) for row in rows]

    def ack(self, message_id: int) -> None:
        """Commit deletion after caller-confirmed delivery; absent IDs are safe."""
        self._validate_integer(message_id, "message_id")
        with self._operation() as connection:
            connection.execute(
                "DELETE FROM outbox_messages WHERE id = ?", (message_id,)
            )

    def count(self) -> int:
        with self._operation() as connection:
            return int(
                connection.execute("SELECT COUNT(*) FROM outbox_messages").fetchone()[0]
            )

    @staticmethod
    def _validate_integer(value: int, name: str) -> None:
        if type(value) is not int or not 0 < value <= 2**63 - 1:
            raise ValueError(f"{name} must be a positive SQLite-sized integer")

    def close(self) -> None:
        """Wait for in-flight operations and close; repeated calls are safe."""
        with self._lock:
            if self._connection is not None:
                try:
                    self._connection.close()
                except sqlite3.Error as exc:
                    raise OutboxError("Could not close outbox") from exc
                self._connection = None

    def __enter__(self) -> Self:
        with self._operation():
            return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
