import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import TIMEOUT_MAX, Event
from time import monotonic
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from energimetrics.helpers.outbox import (
    Outbox,
    OutboxConfig,
    OutboxError,
    OutboxMessage,
)


def rows(config: OutboxConfig) -> list[OutboxMessage]:
    db = sqlite3.connect(config.path)
    db.row_factory = sqlite3.Row
    try:
        return [
            OutboxMessage.model_validate(dict(row))
            for row in db.execute("SELECT * FROM outbox_messages ORDER BY id")
        ]
    finally:
        db.close()


def test_defaults_and_custom_config(tmp_path: Path) -> None:
    config = OutboxConfig(path=tmp_path / "outbox.db")
    assert config.delivered_retention == timedelta(hours=24)
    assert config.cleanup_interval == timedelta(hours=1)
    custom = OutboxConfig.model_validate(
        {"path": config.path, "delivered_retention": "PT2H", "cleanup_interval": 0.02}
    )
    assert custom.delivered_retention == timedelta(hours=2)
    assert custom.cleanup_interval == timedelta(milliseconds=20)


@pytest.mark.parametrize("field", ["delivered_retention", "cleanup_interval"])
@pytest.mark.parametrize("value", [0, -1])
def test_invalid_duration(tmp_path: Path, field: str, value: int) -> None:
    with pytest.raises(ValidationError):
        OutboxConfig.model_validate({"path": tmp_path / "outbox.db", field: value})


def test_ack_retains_original_timestamp_and_pending_order(tmp_path: Path) -> None:
    config = OutboxConfig(path=tmp_path / "outbox.db")
    with Outbox(config) as box:
        messages = [box.enqueue("test", str(i)) for i in range(3)]
        before = datetime.now(UTC)
        box.ack(messages[1].id)
        delivered = rows(config)[1]
        assert delivered.delivered_at is not None
        assert before <= delivered.delivered_at <= datetime.now(UTC)
        assert delivered.delivered_at.tzinfo is UTC
        box.ack(messages[1].id)
        box.ack(999)
        assert rows(config)[1] == delivered
        assert box.pending() == [messages[0], messages[2]]
        assert box.count() == 2
    with Outbox(config) as box:
        assert rows(config)[1] == delivered
        assert box.pending() == [messages[0], messages[2]]


def test_startup_cleanup_and_retention_boundary(tmp_path: Path) -> None:
    config = OutboxConfig(
        path=tmp_path / "outbox.db", delivered_retention=timedelta(hours=2)
    )
    now = datetime.now(UTC)
    cutoff = now - config.delivered_retention
    with Outbox(config) as box:
        messages = [box.enqueue("test", str(i)) for i in range(4)]
        with box._operation() as db:  # pyright: ignore[reportPrivateUsage]
            db.execute(
                "UPDATE outbox_messages SET created_at = ?",
                ((now - timedelta(days=365)).isoformat(),),
            )
            for message, timestamp in zip(
                messages[1:], [cutoff - timedelta(seconds=1), cutoff, now], strict=True
            ):
                db.execute(
                    "UPDATE outbox_messages SET delivered_at = ? WHERE id = ?",
                    (timestamp.isoformat(), message.id),
                )
    with patch("energimetrics.helpers.outbox.outbox.datetime") as clock:
        clock.now.return_value = now
        with Outbox(config) as box:
            assert [m.id for m in rows(config)] == [messages[i].id for i in [0, 2, 3]]
            assert box.count() == 1
            assert box.pending()[0].id == messages[0].id


def test_background_cleanup_and_prompt_concurrent_close(tmp_path: Path) -> None:
    config = OutboxConfig(
        path=tmp_path / "outbox.db",
        delivered_retention=timedelta(hours=1),
        cleanup_interval=timedelta(milliseconds=20),
    )
    box = Outbox(config)
    try:
        pending = box.enqueue("pending", b"keep")
        delivered = box.enqueue("delivered", b"expire")
        box.ack(delivered.id)
        assert len(rows(config)) == 2
        with box._operation() as db:  # pyright: ignore[reportPrivateUsage]
            db.execute(
                "UPDATE outbox_messages SET delivered_at = ? WHERE id = ?",
                ((datetime.now(UTC) - timedelta(days=2)).isoformat(), delivered.id),
            )
        deadline = monotonic() + 5
        while len(rows(config)) != 1 and monotonic() < deadline:
            Event().wait(0.01)
        assert rows(config) == [pending]
    finally:
        box.close()
    # The default hour-long wait must also be interruptible.
    box = Outbox(OutboxConfig(path=config.path))
    started = monotonic()
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(box.close) for _ in range(3)]
        for future in futures:
            future.result(timeout=1)
    assert monotonic() - started < 1
    assert not box._worker.is_alive()  # pyright: ignore[reportPrivateUsage]


def test_cleanup_failure_retries_without_breaking_operations(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    config = OutboxConfig(
        path=tmp_path / "outbox.db",
        delivered_retention=timedelta(hours=1),
        cleanup_interval=timedelta(milliseconds=20),
    )
    with Outbox(config) as box:
        message = box.enqueue("test", b"payload")
        box.ack(message.id)
        with box._operation() as db:  # pyright: ignore[reportPrivateUsage]
            db.execute(
                "UPDATE outbox_messages SET delivered_at = ?",
                ((datetime.now(UTC) - timedelta(days=2)).isoformat(),),
            )
    db = sqlite3.connect(config.path)
    try:
        db.execute(
            "CREATE TRIGGER fail_cleanup BEFORE DELETE ON outbox_messages "
            "BEGIN SELECT RAISE(ABORT, 'cleanup failed'); END"
        )
        db.commit()
        with Outbox(config) as box:
            assert "Outbox retention cleanup failed" in caplog.text
            message = box.enqueue("still working", b"ok")
            assert box.pending() == [message]
            box.ack(message.id)
            assert box.count() == 0
            with box._operation() as connection:  # pyright: ignore[reportPrivateUsage]
                connection.execute(
                    "UPDATE outbox_messages SET delivered_at = ? "
                    "WHERE delivered_at IS NOT NULL",
                    ((datetime.now(UTC) - timedelta(days=2)).isoformat(),),
                )
            db.execute("DROP TRIGGER fail_cleanup")
            db.commit()
            deadline = monotonic() + 5
            while rows(config) and monotonic() < deadline:
                Event().wait(0.01)
            assert rows(config) == []
    finally:
        db.close()


def test_legacy_schema_migration(tmp_path: Path) -> None:
    config = OutboxConfig(path=tmp_path / "outbox.db")
    db = sqlite3.connect(config.path)
    try:
        db.execute(
            "CREATE TABLE outbox_messages (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "destination TEXT NOT NULL, payload BLOB NOT NULL, "
            "created_at TEXT NOT NULL)"
        )
        db.execute(
            "INSERT INTO outbox_messages VALUES (?, ?, ?, ?)",
            (42, "legacy", b"unchanged", datetime.now(UTC).isoformat()),
        )
        db.commit()
    finally:
        db.close()
    with Outbox(config) as box:
        with box._operation() as db:  # pyright: ignore[reportPrivateUsage]
            assert "outbox_messages_delivered_at" in {
                row["name"] for row in db.execute("PRAGMA index_list(outbox_messages)")
            }
        message = box.pending()[0]
        assert message.id == 42
        assert message.payload == b"unchanged"
        assert message.delivered_at is None
        box.ack(42)
        assert box.enqueue("new", b"new").id > 42
    with Outbox(config) as box:
        assert box.count() == 1
        assert rows(config)[0].delivered_at is not None


@pytest.mark.parametrize("operation", ["pending", "close"])
def test_maintenance_contention_is_brief(tmp_path: Path, operation: str) -> None:
    config = OutboxConfig(
        path=tmp_path / "outbox.db",
        cleanup_interval=timedelta(milliseconds=20),
    )
    box = Outbox(config)
    writer = sqlite3.connect(config.path)
    entered = Event()

    def trace(statement: str) -> None:
        if statement == "BEGIN IMMEDIATE":
            entered.set()

    try:
        message = box.enqueue("pending", b"keep")
        with box._operation() as db:  # pyright: ignore[reportPrivateUsage]
            # Verify restoration of the actual setting, rather than a default.
            db.execute("PRAGMA busy_timeout=4321")
            db.set_trace_callback(trace)
            writer.execute("BEGIN IMMEDIATE")
        assert entered.wait(2), "Maintenance did not attempt a write"
        started = monotonic()
        if operation == "close":
            box.close()
            assert not box._worker.is_alive()  # pyright: ignore[reportPrivateUsage]
        else:
            assert box.pending() == [message]
            with box._operation() as db:  # pyright: ignore[reportPrivateUsage]
                assert db.execute("PRAGMA busy_timeout").fetchone()[0] == 4321
        assert monotonic() - started < 1
        writer.rollback()
        if operation == "pending":
            box.ack(message.id)
            assert box.count() == 0
            # Successful cleanup must restore the timeout as well.
            box._cleanup()  # pyright: ignore[reportPrivateUsage]
            with box._operation() as db:  # pyright: ignore[reportPrivateUsage]
                assert db.execute("PRAGMA busy_timeout").fetchone()[0] == 4321
    finally:
        writer.close()
        box.close()


def test_retention_queries_use_index(tmp_path: Path) -> None:
    config = OutboxConfig(path=tmp_path / "outbox.db")
    with Outbox(config) as box:
        with box._operation() as db:  # pyright: ignore[reportPrivateUsage]
            timestamp = datetime.now(UTC).isoformat(timespec="microseconds")
            db.executemany(
                "INSERT INTO outbox_messages "
                "(destination, payload, created_at, delivered_at) VALUES (?, ?, ?, ?)",
                [("history", b"payload", timestamp, timestamp)] * 1000,
            )
        messages = [box.enqueue("pending", str(i)) for i in range(3)]
        statements: list[str] = []
        with box._operation() as db:  # pyright: ignore[reportPrivateUsage]
            db.set_trace_callback(statements.append)
        try:
            assert box.pending(2) == messages[:2]
            assert box.count() == 3
            box._cleanup()  # pyright: ignore[reportPrivateUsage]
        finally:
            with box._operation() as db:  # pyright: ignore[reportPrivateUsage]
                db.set_trace_callback(None)
        queries = [sql for sql in statements if sql.startswith(("SELECT", "DELETE"))]
        assert len(queries) == 3
        with box._operation() as db:  # pyright: ignore[reportPrivateUsage]
            for query in queries:
                plan = " ".join(
                    row["detail"] for row in db.execute("EXPLAIN QUERY PLAN " + query)
                )
                assert "SEARCH" in plan
                assert "outbox_messages_delivered_at" in plan
                assert "SCAN" not in plan
                assert "TEMP B-TREE" not in plan


@pytest.mark.parametrize("stop_early", [False, True])
def test_cleanup_batches_release_lock_and_honor_close(
    tmp_path: Path, stop_early: bool
) -> None:
    config = OutboxConfig(path=tmp_path / "outbox.db")
    box = Outbox(config)
    batch_finished = Event()
    resume = Event()
    original_batch = box._cleanup_batch  # pyright: ignore[reportPrivateUsage]
    deleted: list[int] = []

    def batch(cutoff: datetime) -> int:
        count = original_batch(cutoff)
        deleted.append(count)
        if len(deleted) == 1:
            batch_finished.set()
            assert resume.wait(5)
        return count

    try:
        with box._operation() as db:  # pyright: ignore[reportPrivateUsage]
            timestamp = (datetime.now(UTC) - timedelta(days=2)).isoformat()
            db.executemany(
                "INSERT INTO outbox_messages "
                "(destination, payload, created_at, delivered_at) VALUES (?, ?, ?, ?)",
                [("expired", b"payload", timestamp, timestamp)] * 250,
            )
        with (
            patch.object(box, "_cleanup_batch", side_effect=batch),
            ThreadPoolExecutor(max_workers=1) as pool,
        ):
            cleanup = pool.submit(box._cleanup)  # pyright: ignore[reportPrivateUsage]
            try:
                assert batch_finished.wait(2)
                # The first batch committed, and application operations can proceed.
                assert len(rows(config)) == 150
                message = box.enqueue("pending", b"keep")
                assert box.pending() == [message]
                if stop_early:
                    box.close()
            finally:
                resume.set()
            cleanup.result(timeout=2)
        if stop_early:
            assert deleted == [100]
            assert len(rows(config)) == 151
        else:
            assert deleted == [100, 100, 50]
            assert rows(config) == [message]
    finally:
        box.close()


@pytest.mark.parametrize("failure_point", ["Thread", "Thread.start"])
def test_worker_startup_failure_closes_connection(
    tmp_path: Path, failure_point: str
) -> None:
    config = OutboxConfig(path=tmp_path / "outbox.db")
    connection = sqlite3.connect(config.path)
    failure = RuntimeError("cannot start worker")
    with (
        patch(
            "energimetrics.helpers.outbox.outbox.sqlite3.connect",
            return_value=connection,
        ),
        patch(
            "energimetrics.helpers.outbox.outbox." + failure_point,
            side_effect=failure,
        ),
        pytest.raises(OutboxError, match="Could not start outbox maintenance") as error,
    ):
        Outbox(config)
    assert error.value.__cause__ is failure
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connection.execute("SELECT 1")
    with Outbox(config) as box:
        assert box.count() == 0


def test_retention_before_datetime_min_keeps_messages(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    config = OutboxConfig(
        path=tmp_path / "outbox.db", delivered_retention=timedelta.max
    )
    with Outbox(config) as box:
        pending = box.enqueue("pending", b"keep")
        delivered = box.enqueue("delivered", b"keep too")
        box.ack(delivered.id)
    with Outbox(config) as box:
        box._cleanup()  # pyright: ignore[reportPrivateUsage]
        assert len(rows(config)) == 2
        assert box.pending() == [pending]
    assert "Outbox retention cleanup failed" not in caplog.text


@pytest.mark.parametrize("stop_after", [1, 2, 4])
def test_long_interval_waits_in_interruptible_chunks(
    tmp_path: Path, stop_after: int
) -> None:
    # Exercise the wait loop without starting a thread or waiting centuries.
    box = Outbox.__new__(Outbox)
    box._config = OutboxConfig(  # pyright: ignore[reportPrivateUsage]
        path=tmp_path / "outbox.db",
        cleanup_interval=timedelta(seconds=TIMEOUT_MAX * 2 + 0.5),
    )
    box._stop = Event()  # pyright: ignore[reportPrivateUsage]
    with (
        patch.object(
            box._stop,  # pyright: ignore[reportPrivateUsage]
            "wait",
            side_effect=[False] * (stop_after - 1) + [True],
        ) as wait,
        patch.object(box, "_cleanup") as cleanup,
    ):
        box._maintain()  # pyright: ignore[reportPrivateUsage]
    expected = [TIMEOUT_MAX, TIMEOUT_MAX, 0.5, TIMEOUT_MAX]
    assert [call.args[0] for call in wait.call_args_list] == expected[:stop_after]
    assert cleanup.call_count == (1 if stop_after == 4 else 0)


def test_close_joins_actual_worker_between_cleanup_batches(tmp_path: Path) -> None:
    config = OutboxConfig(
        path=tmp_path / "outbox.db",
        cleanup_interval=timedelta(milliseconds=20),
    )
    box = Outbox(config)
    batch_finished = Event()
    original_batch = box._cleanup_batch  # pyright: ignore[reportPrivateUsage]
    deleted: list[int] = []

    def batch(cutoff: datetime) -> int:
        count = original_batch(cutoff)
        if count:
            deleted.append(count)
            batch_finished.set()
            # Pause the real worker outside the SQLite lock until close signals it.
            box._stop.wait(5)  # pyright: ignore[reportPrivateUsage]
        return count

    try:
        with patch.object(box, "_cleanup_batch", side_effect=batch):
            with box._operation() as db:  # pyright: ignore[reportPrivateUsage]
                timestamp = (datetime.now(UTC) - timedelta(days=2)).isoformat()
                db.executemany(
                    "INSERT INTO outbox_messages "
                    "(destination, payload, created_at, delivered_at) "
                    "VALUES (?, ?, ?, ?)",
                    [("expired", b"payload", timestamp, timestamp)] * 250,
                )
            assert batch_finished.wait(2)
            started = monotonic()
            box.close()
            assert monotonic() - started < 1
        assert not box._worker.is_alive()  # pyright: ignore[reportPrivateUsage]
        assert deleted == [100]
        assert len(rows(config)) == 150
    finally:
        box.close()
