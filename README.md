# Energimetrics Python outbox helper

A small, transport-independent SQLite outbox at `energimetrics.helpers.outbox`.
It protects outbound observations during external outages: **persist first,
deliver second**. It never publishes messages, interprets payloads, or owns delivery workers
or delivery retry policy.

## Usage

The following is consuming-application code. `deliver` and `DeliveryError` belong
to the application, not this helper. `deliver()` must wait for the application's
required delivery confirmation before returning successfully. For asynchronous
transports, acknowledge only from the confirmed-success path, not when sending
is merely scheduled.

```python
import json
from datetime import UTC, datetime
from pathlib import Path

from energimetrics.helpers.outbox import Outbox, OutboxConfig

payload = json.dumps({"state": True, "timestamp": datetime.now(UTC).isoformat()})

with Outbox(OutboxConfig(path=Path("/var/lib/energimetrics/outbox.db"))) as outbox:
    outbox.enqueue(destinations=["primary"], payload=payload)

    for message in outbox.pending(destinations=["primary"]):
        try:
            deliver(destination=message.destination, payload=message.payload)
        except DeliveryError:
            break
        else:
            outbox.ack(message.id)
```

Drain existing pending messages on startup even if there are no new observations.
The caller schedules subsequent delivery attempts.

## API

- `OutboxConfig(path: Path)`: frozen Pydantic configuration. String paths are also
  accepted at runtime (including when loading configuration with `model_validate`).
  The parent directory must already exist; a missing parent causes construction
  to fail with `OutboxError`. Empty paths, `:memory:`, and NULs are rejected.
- `Outbox(config)`: opens or creates the database.
- `enqueue(destinations: list[str], payload: bytes | str) -> list[OutboxMessage]`:
  atomically commits one message per destination before returning. Returns a list
  in destination input order, even for one destination. Strings are UTF-8 encoded;
  bytes remain exact, including non-UTF-8 data. All rows share a creation timestamp.
- `pending(limit: int = 100, *, destinations: list[str] | None = None) -> list[OutboxMessage]`:
  a bounded snapshot in ascending persistent ID order. Omitted or `None` selects
  all destinations. Filtering happens before the limit; the limit is shared across
  all selected destinations. Limit must be a positive signed 64-bit integer, not a bool.
- `ack(message_id: int) -> None`: commits a timezone-aware UTC delivery timestamp. Repeating a positive ID is safe.
- `count() -> int`: current pending count.
- `close() -> None`: closes resources; repeated calls are safe. Context managers
  close on both success and exceptions. Operations after close raise `OutboxError`.

Both APIs require destination filters/targets to be non-empty lists of strings,
with no duplicates or empty/whitespace-only names. Names are otherwise preserved
exactly. Invalid types raise `TypeError`; invalid values raise `ValueError`.

`OutboxMessage` is a frozen Pydantic model with `id`, `destination`, `payload`
(bytes), timezone-aware UTC `created_at`, and nullable UTC `delivered_at`. Its timestamp is operational
metadata recording entry into the outbox, not the observation time. The application
creates the authoritative observation timestamp inside its payload; the outbox
never parses or changes it. Ordering uses insertion IDs, even if the wall clock
moves backwards or timestamps are identical. IDs are not reused after deletion.

## Multiple destinations and API migration

This is a breaking API change: replace `enqueue(destination="a", payload=data)`
with `enqueue(destinations=["a"], payload=data)`. The return value is always a list;
use `[0]` when the caller needs the single message. Existing stored rows need no
data migration. Opening a database automatically adds the pending-destination index.

```python
messages = outbox.enqueue(destinations=["database-a", "database-b"], payload=payload)

# exporter_a is application-owned; send() must await acceptance or raise.
for message in outbox.pending(destinations=["database-a"], limit=100):
    exporter_a.send(message.payload)
    outbox.ack(message.id)
```

If B is offline, only A's rows are acknowledged. B's rows survive restart and are
retrieved with `pending(destinations=["database-b"])` when its worker retries.
An offline B backlog does not hide A's newer messages, because SQL filters before
applying the limit. Destination strings are opaque routing keys, not validated URLs.

All target rows are inserted in one transaction: a later insert failure rolls back
earlier inserts. Commit failures can have an uncertain outcome, as described below.
Each row has an independent ID and acknowledgement. The payload is stored once per
destination; `count()` counts pending delivery rows, not unique observations.

## Durability and delivery semantics

Successful `enqueue()` means its SQLite transaction committed. Messages survive
closing/reopening, process crashes, application/container restarts and recreation,
and host restarts when stored on suitable persistent local storage. The settings
are `journal_mode=WAL`, `synchronous=FULL`, and `fullfsync=ON` (stronger flushing on
macOS, ignored on platforms without that facility). WAL and FULL synchronization
are verified at open. FULL synchronizes the WAL at each commit, prioritizing
persistence over throughput; see [SQLite synchronization documentation](https://www.sqlite.org/pragma.html#pragma_synchronous).

The deployment/environment must create the parent directory and persistently
mount it **before constructing Outbox**. For example,
`/var/lib/energimetrics/outbox.db` requires `/var/lib/energimetrics` to already
exist. Outbox creates the SQLite database file, but never creates directories.
Missing parents cause `OutboxError` without creating any part of the path.

This keeps deployment-created directory entries outside the helper's storage
responsibilities and prevents it from silently creating an ephemeral directory
when an expected volume is absent. An existing directory alone does not prove
that the persistent volume is mounted; the deployment must ensure that it is.

The filesystem and hardware must honour SQLite's sync requests. Use local storage
with proper locking, not a network filesystem. Keep the **whole database directory**
persistent: the `-wal` file may contain committed messages not yet checkpointed
into the main database; do not delete it or copy only the live `.db` file. See
[SQLite WAL documentation](https://www.sqlite.org/wal.html). For example, the
consuming deployment must provision `/opt/energimetrics/binaryinput` on persistent
host storage before mounting it:

```yaml
volumes:
  - /opt/energimetrics/binaryinput:/var/lib/energimetrics
```

and configure:

```yaml
outbox:
  path: /var/lib/energimetrics/outbox.db
```

The helper does not manage Docker volumes. Acknowledged messages are retained
as a temporary recovery journal. They no longer appear in `pending()` or `count()`;
repeated acknowledgements preserve the original delivery timestamp.

Retention is entirely internal to the helper and requires no application
integration: applications continue to call only `enqueue()`, `pending()`, and
`ack()`. No purge calls, cleanup scheduling, or delivered-row management are needed.
`OutboxConfig.delivered_retention` defaults to 24 hours and `cleanup_interval`
defaults to 1 hour. Both are positive `datetime.timedelta` values; Pydantic also
accepts duration strings or seconds when loading configuration. For example:

```yaml
outbox:
  path: /var/lib/energimetrics/outbox.db
  delivered_retention: PT24H
  cleanup_interval: PT1H
```

Cleanup runs on startup and then on an internal maintenance thread at the configured
interval. It deletes only rows whose delivery timestamp is strictly older than
the retention cutoff; pending messages are never expired. Delivered rows may remain
until the next successful cleanup. Cleanup failures are logged and retried at the
next interval without failing application operations. Cleanup uses a short SQLite
lock timeout so contention does not hold up delivery or shutdown for the normal
write timeout. Expired rows are deleted in batches of at most 100, releasing the
connection lock and checking for shutdown between batches. Persistence failures still
raise `OutboxError`. `close()` interrupts the maintenance wait and joins the worker.
Startup cleanup completes synchronously, so a large expired backlog can delay
construction. Batching limits rows per transaction, not elapsed time: large
payloads or slow storage can still delay an individual batch and shutdown.
SQLite reuses pages freed by deletion; retention does not necessarily shrink the
database file or return disk space to the operating system.

Existing databases are migrated automatically, preserving every pending message.
Its schema is deliberately small:

```sql
CREATE TABLE outbox_messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    destination TEXT NOT NULL,
    payload BLOB NOT NULL,
    created_at TEXT NOT NULL,
    delivered_at TEXT
);
CREATE INDEX outbox_messages_delivered_at ON outbox_messages (delivered_at);
CREATE INDEX outbox_messages_pending_destination
    ON outbox_messages (destination, id) WHERE delivered_at IS NULL;
```

The caller controls acknowledgement. `ack()` means only that the caller considers
delivery complete; it does not independently verify that a downstream service
processed or stored the payload. Transport-specific confirmation belongs to the
caller.

This supports **at-least-once**, not exactly-once, delivery. If delivery succeeds
but the process crashes before acknowledgement commits, the message is replayed.
Consumers must tolerate duplicates. An operation that raises may have an uncertain
commit outcome; do not assume it succeeded, and account for possible duplicates
when retrying. Enqueue failures must be handled by the application: an observation
that could not be persisted is not protected by the outbox.

SQLite and filesystem/open failures are chained into `OutboxError`. Invalid API
argument types/values raise `TypeError`/`ValueError`; invalid Pydantic models raise
`ValidationError`. Payloads are never logged; the helper configures no logging.

## Concurrency

Share one `Outbox` across producer callbacks within a process. A reentrant lock
serializes all connection operations, complete transactions, internal cleanup,
and close. SQLite's cross-thread connection check is disabled only because this
lock protects access. Delivery happens outside the lock.

`pending()` does not claim or reserve messages. Multiple delivery workers could
publish the same snapshot and reorder delivery. Use one worker per destination
with disjoint destination filters (or one worker for the entire outbox). Overlapping
filters and unfiltered workers must not run concurrently when independent delivery
is required. Filtering does not claim or reserve rows. Each destination retains
insertion order; there is no cross-destination delivery ordering guarantee.
Concurrent producers are ordered by their serialized insertions, not by an inferred
observation time. No distributed coordination or process-fork sharing
is supported; open a new instance after restarting a process.

## Installation and development

Python 3.14 or newer is required, matching the neighbouring Energimetrics helpers.
Install from the repository in a consuming uv project:

```sh
uv add 'energimetrics.helpers.outbox @ git+https://github.com/EnergiMetrics/py-helpers-outbox.git'
```

For a fresh development clone:

```sh
uv sync --locked
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pre-commit run --all-files
uv build
```

Use `uv sync` when intentionally updating the environment/lockfile. `uv.lock` is
version-controlled. The only runtime dependency is Pydantic. The `dev` dependency
group contains pytest, pytest-cov, Ruff, Pyright and pre-commit. The package uses
uv_build and the `src/energimetrics/helpers/outbox` namespace layout, with typed
exports. CI runs lint, formatting, tests, strict type checks, and a package build.

Tests use temporary local databases and no external services. They exercise exact
payload preservation, restart and abrupt-process recovery, ordering, committed
visibility, acknowledgement, failure/rollback, and concurrent production/delivery.
Power-loss resilience relies on SQLite and the storage stack; a unit test cannot
simulate physical storage losing power.
