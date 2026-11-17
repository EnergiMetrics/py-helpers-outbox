import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from threading import Event

import pytest

from energimetrics.helpers.outbox import Outbox, OutboxConfig, OutboxError


@pytest.fixture
def config(tmp_path: Path) -> OutboxConfig:
    return OutboxConfig(path=tmp_path / "outbox.db")


def test_atomic_fanout_and_independent_restart(config: OutboxConfig) -> None:
    with Outbox(config) as box:
        messages = box.enqueue(destinations=["b", "a"], payload="café")
        assert [m.destination for m in messages] == ["b", "a"]
        assert messages[0].id < messages[1].id
        assert messages[0].created_at == messages[1].created_at
        assert all(m.payload == "café".encode() for m in messages)
        with closing(sqlite3.connect(config.path)) as db, db:
            assert db.execute("SELECT COUNT(*) FROM outbox_messages").fetchone()[0] == 2
        box.ack(messages[1].id)
        assert box.pending(destinations=["a"]) == []
        assert box.pending(destinations=["b"]) == [messages[0]]
    with Outbox(config) as box:
        assert box.pending() == [messages[0]]
        box.ack(messages[0].id)
        assert box.count() == 0


def test_later_insert_failure_rolls_back_entire_fanout(config: OutboxConfig) -> None:
    with Outbox(config) as box:
        original = box.enqueue(["existing"], b"keep")
        with closing(sqlite3.connect(config.path)) as db, db:
            db.execute(
                "CREATE TRIGGER reject_b BEFORE INSERT ON outbox_messages "
                "WHEN NEW.destination = 'b' "
                "BEGIN SELECT RAISE(ABORT, 'b unavailable'); END"
            )
        with pytest.raises(OutboxError):
            box.enqueue(["a", "b"], b"must roll back")
        assert box.pending() == original
    with Outbox(config) as box:
        assert box.pending() == original


def test_fanout_commit_failure_rolls_back(config: OutboxConfig) -> None:
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
        try:
            with pytest.raises(OutboxError):
                box.enqueue(["a", "b"], b"fail")
        finally:
            db.set_authorizer(None)
        assert box.count() == 0


@pytest.mark.parametrize(
    ("destinations", "error"),
    [
        ("a", TypeError),
        (("a",), TypeError),
        (5, TypeError),
        (["a", 5], TypeError),
        ([], ValueError),
        (["a", "a"], ValueError),
        (["a", ""], ValueError),
        (["a", "  "], ValueError),
    ],
)
def test_invalid_destinations(
    config: OutboxConfig, destinations: object, error: type[Exception]
) -> None:
    with Outbox(config) as box:
        with pytest.raises(error):
            box.enqueue(destinations, b"x")  # pyright: ignore[reportArgumentType]
        with pytest.raises(error):
            box.pending(destinations=destinations)  # pyright: ignore[reportArgumentType]
        assert box.count() == 0


def test_filter_before_limit_and_global_insertion_order(config: OutboxConfig) -> None:
    unusual = "x') OR 1=1; --"
    with Outbox(config) as box:
        for _ in range(105):
            box.enqueue(["offline"], b"backlog")
        first = box.enqueue(["a", "b"], b"first")
        second = box.enqueue(["a", unusual], b"second")
        box.ack(first[1].id)
        assert box.pending(1, destinations=["a"]) == first[:1]
        assert box.pending(destinations=["missing"]) == []
        assert box.pending(destinations=[unusual]) == second[1:]
        assert box.pending(2, destinations=[unusual, "a"]) == [first[0], second[0]]
        assert box.pending(destinations=None) == box.pending()


def test_index_added_to_existing_database(config: OutboxConfig) -> None:
    with Outbox(config) as box:
        original = box.enqueue(["a"], b"unchanged")
    with closing(sqlite3.connect(config.path)) as db, db:
        db.execute("DROP INDEX outbox_messages_pending_destination")
    with Outbox(config) as box:
        assert box.pending(destinations=["a"]) == original
        with box._operation() as db:  # pyright: ignore[reportPrivateUsage]
            plan = " ".join(
                row["detail"]
                for row in db.execute(
                    "EXPLAIN QUERY PLAN SELECT * FROM outbox_messages "
                    "WHERE delivered_at IS NULL AND destination IN (?) "
                    "ORDER BY id LIMIT ?",
                    ("a", 100),
                )
            )
        assert "outbox_messages_pending_destination" in plan


def test_offline_destination_does_not_block_active_worker(config: OutboxConfig) -> None:
    done = Event()
    with Outbox(config) as box, ThreadPoolExecutor(max_workers=3) as pool:

        def produce() -> None:
            for i in range(40):
                box.enqueue(["offline", "online"], str(i))

        def deliver(destination: str) -> list[int]:
            delivered: list[int] = []
            while True:
                pending = box.pending(7, destinations=[destination])
                for message in pending:
                    delivered.append(int(message.payload))
                    box.ack(message.id)
                if done.is_set() and not pending:
                    return delivered
                done.wait(0.001)

        # Simulate an outage by not running an offline delivery worker yet.
        worker = pool.submit(deliver, "online")
        producer = pool.submit(produce)
        try:
            producer.result(timeout=10)
        finally:
            done.set()
        assert worker.result(timeout=10) == list(range(40))
        assert box.count() == 40
        assert box.pending(destinations=["online"]) == []
    with Outbox(config) as box:
        assert [int(m.payload) for m in box.pending(destinations=["offline"])] == list(
            range(40)
        )
        for message in box.pending(destinations=["offline"]):
            box.ack(message.id)
        assert box.count() == 0


def test_disjoint_workers_and_producer(config: OutboxConfig) -> None:
    done = Event()
    with Outbox(config) as box, ThreadPoolExecutor(max_workers=3) as pool:

        def deliver(destination: str) -> list[int]:
            values: list[int] = []
            while True:
                pending = box.pending(5, destinations=[destination])
                for message in pending:
                    values.append(int(message.payload))
                    box.ack(message.id)
                if done.is_set() and not pending:
                    return values
                done.wait(0.001)

        workers = [pool.submit(deliver, destination) for destination in ["a", "b"]]
        try:
            for i in range(30):
                box.enqueue(["a", "b"], str(i))
        finally:
            done.set()
        assert [worker.result(timeout=10) for worker in workers] == [
            list(range(30))
        ] * 2
        assert box.count() == 0
