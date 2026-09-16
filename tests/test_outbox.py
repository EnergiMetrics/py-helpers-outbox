import os
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from energimetrics.helpers.outbox import (
    Outbox,
    OutboxConfig,
    OutboxError,
    OutboxMessage,
)


@pytest.fixture
def config(tmp_path: Path) -> OutboxConfig:
    return OutboxConfig(path=tmp_path / "outbox.db")


def test_create_and_settings(config: OutboxConfig) -> None:
    assert config.path.parent.is_dir()
    assert not config.path.exists()
    with Outbox(config) as box:
        assert config.path.is_file()
        assert box.count() == 0
        assert box.pending() == []
        with box._operation() as db:  # pyright: ignore[reportPrivateUsage]
            assert db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
            assert db.execute("PRAGMA synchronous").fetchone()[0] == 2
            assert db.execute("PRAGMA fullfsync").fetchone()[0] == 1


def test_missing_parent_is_not_created(tmp_path: Path) -> None:
    parent = tmp_path / "missing" / "nested"
    config = OutboxConfig(path=parent / "outbox.db")

    with pytest.raises(OutboxError, match="Could not open durable outbox"):
        Outbox(config)

    assert not parent.exists()
    assert not parent.parent.exists()
    assert not config.path.exists()


@pytest.mark.parametrize("payload", [b"\x00\xff\xfe", b"", b"not json", "café ☀"])
def test_payload_and_committed_row(config: OutboxConfig, payload: bytes | str) -> None:
    destination = "a/' ; DROP TABLE outbox_messages; --"
    before = datetime.now(UTC)
    with Outbox(config) as box:
        message = box.enqueue(destination, payload)
        assert isinstance(message, OutboxMessage)
        assert message.payload == (
            payload.encode() if isinstance(payload, str) else payload
        )
        assert message.destination == destination
        assert before <= message.created_at <= datetime.now(UTC)
        assert message.created_at.tzinfo is UTC
        assert box.pending() == [message]
        # An independent connection can see the row before close().
        db = sqlite3.connect(config.path)
        try:
            row = db.execute(
                "SELECT id, destination, payload, typeof(payload) FROM outbox_messages"
            ).fetchone()
            assert row == (message.id, destination, message.payload, "blob")
        finally:
            db.close()


def test_restart_ack_and_ids(config: OutboxConfig) -> None:
    with Outbox(config) as box:
        first = box.enqueue("example/one", b"first")
        second = box.enqueue("example/two", b"second")
        assert box.count() == 2
    with Outbox(config) as box:
        assert box.pending() == [first, second]
        box.ack(first.id)
        box.ack(first.id)
        assert box.count() == 1
        box.ack(second.id)
        assert box.count() == 0
        third = box.enqueue("three", b"third")
        assert third.id > second.id
        box.ack(first.id)
        assert box.pending() == [third]
    with Outbox(config) as box:
        assert box.pending() == [third]


def test_real_observation(config: OutboxConfig) -> None:
    payload = b'{"state":true,"timestamp":"2026-09-16T16:42:31.483Z"}'
    destination = "energimetrics/plantroom-pi-01/binaryinput/dhw_diverter"
    with Outbox(config) as box:
        message = box.enqueue(destination, payload)
    with Outbox(config) as box:
        assert box.pending() == [message]
        assert box.pending()[0].payload == payload
        assert box.pending()[0].destination == destination


def test_order_and_limit(config: OutboxConfig) -> None:
    with Outbox(config) as box:
        messages = [box.enqueue("test", str(i)) for i in range(105)]
        # Simulate identical timestamps: ordering depends only on persistent IDs.
        with box._operation() as db:  # pyright: ignore[reportPrivateUsage]
            db.execute(
                "UPDATE outbox_messages SET created_at = ?",
                (datetime.now(UTC).isoformat(),),
            )
        assert [m.id for m in box.pending()] == [m.id for m in messages[:100]]
        assert [m.id for m in box.pending(3)] == [m.id for m in messages[:3]]
        assert len(box.pending(200)) == 105


@pytest.mark.parametrize("value", [0, -1, True, 1.5, "3", 2**63])
def test_invalid_integers(config: OutboxConfig, value: object) -> None:
    with Outbox(config) as box:
        with pytest.raises(ValueError):
            box.pending(value)  # pyright: ignore[reportArgumentType]
        with pytest.raises(ValueError):
            box.ack(value)  # pyright: ignore[reportArgumentType]


@pytest.mark.parametrize("path", ["", ".", ":memory:", "bad\x00path"])
def test_invalid_path(path: str) -> None:
    with pytest.raises(ValidationError):
        OutboxConfig.model_validate({"path": path})


def test_close_and_context_exception(config: OutboxConfig) -> None:
    box = Outbox(config)
    with pytest.raises(RuntimeError), box:
        box.enqueue("test", b"retained")
        raise RuntimeError("caller failed")
    box.close()
    for operation in [
        box.count,
        box.pending,
        lambda: box.enqueue("x", b"x"),
        lambda: box.ack(1),
        box.__enter__,
    ]:
        with pytest.raises(OutboxError, match="closed"):
            operation()
    with Outbox(config) as reopened:
        assert reopened.count() == 1


def test_open_failures(tmp_path: Path) -> None:
    with pytest.raises(OutboxError):
        Outbox(OutboxConfig(path=tmp_path))
    file = tmp_path / "file"
    file.write_bytes(b"not sqlite")
    with pytest.raises(OutboxError):
        Outbox(OutboxConfig(path=file))
    with pytest.raises(OutboxError):
        Outbox(OutboxConfig(path=file / "outbox.db"))


def test_write_failure(config: OutboxConfig) -> None:
    with Outbox(config) as box:
        with box._operation() as db:  # pyright: ignore[reportPrivateUsage]
            db.execute("PRAGMA query_only=ON")
        with pytest.raises(OutboxError) as error:
            box.enqueue("test", b"must fail")
        assert isinstance(error.value.__cause__, sqlite3.Error)
        assert box.count() == 0


def test_commit_failure_rolls_back(config: OutboxConfig) -> None:
    with Outbox(config) as box:
        with box._operation() as db:  # pyright: ignore[reportPrivateUsage]

            def deny_commit(
                action: int,
                arg: str | None,
                _b: str | None,
                _c: str | None,
                _d: str | None,
            ) -> int:
                if action == sqlite3.SQLITE_TRANSACTION and arg == "COMMIT":
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            db.set_authorizer(deny_commit)
        with pytest.raises(OutboxError):
            box.enqueue("failed", b"no success")
        db.set_authorizer(None)
        assert box.count() == 0
        message = box.enqueue("retry", b"ok")
        with box._operation() as db:  # pyright: ignore[reportPrivateUsage]
            db.set_authorizer(deny_commit)
        with pytest.raises(OutboxError):
            box.ack(message.id)
        db.set_authorizer(None)
        assert box.pending() == [message]


def test_unsupported_durability(config: OutboxConfig) -> None:
    with patch("energimetrics.helpers.outbox.outbox.sqlite3.connect") as connect:
        connect.return_value.execute.return_value.fetchone.return_value = ("delete",)
        with pytest.raises(OutboxError):
            Outbox(config)
        connect.return_value.close.assert_called_once()


def test_invalid_input(config: OutboxConfig) -> None:
    with Outbox(config) as box:
        with pytest.raises(TypeError):
            box.enqueue(5, b"x")  # pyright: ignore[reportArgumentType]
        with pytest.raises(TypeError):
            box.enqueue("x", 5)  # pyright: ignore[reportArgumentType]
        assert box.count() == 0


def test_concurrent_producers_and_delivery(config: OutboxConfig) -> None:
    config = config.model_copy(update={"cleanup_interval": timedelta(milliseconds=1)})
    done = Event()
    with Outbox(config) as box, ThreadPoolExecutor(max_workers=4) as pool:

        def produce(producer: int) -> None:
            for number in range(30):
                box.enqueue(str(producer), str(number))

        def deliver() -> list[OutboxMessage]:
            delivered: list[OutboxMessage] = []
            while not done.is_set() or box.count():
                for message in box.pending(7):
                    delivered.append(message)
                    box.ack(message.id)
                done.wait(0.001)
            return delivered

        worker = pool.submit(deliver)
        producers = [pool.submit(produce, i) for i in range(3)]
        try:
            for producer in producers:
                producer.result(timeout=20)
        finally:
            done.set()
        delivered = worker.result(timeout=20)
        assert len(delivered) == 90
        assert [m.id for m in delivered] == sorted({m.id for m in delivered})
        for i in range(3):
            assert [
                int(m.payload) for m in delivered if m.destination == str(i)
            ] == list(range(30))
        assert box.count() == 0


def test_abrupt_process_exit(config: OutboxConfig) -> None:
    script = """
import os, sys
from energimetrics.helpers.outbox import Outbox, OutboxConfig
box = Outbox(OutboxConfig(path=sys.argv[1]))
box.enqueue('one', b'first')
box.enqueue('two', b'second')
os._exit(0)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(config.path)],
        env=os.environ.copy(),
        check=False,
        timeout=20,
    )
    assert result.returncode == 0
    with Outbox(config) as box:
        assert [m.payload for m in box.pending()] == [b"first", b"second"]
