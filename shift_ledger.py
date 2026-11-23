#!/usr/bin/env python3
"""shift_ledger.py — offline per-device shift ledger for cumulative counters.

Field terminals record a device's *cumulative* work counter, not per-shift
output.  This tool receives the JSONL view produced by inspect_rebuild.py
(one file per `apply`), keeps a versioned SQLite ledger of counter readings,
and exports per-shift accounting rows as JSONL (`query`).

Only lines with "type":"record" carry a message in their "object"; every
other line type yields no reading — no zero filling, no invented device or
time.  A message is:

    device      non-empty string
    reading_id  non-empty string
    revision    integer, 1 .. 2^63-1
    op          "put" | "delete"
    put only:   epoch   non-empty string
                time    UTC unix seconds, integer 0 .. 2^53-1
                counter cumulative count, integer 0 .. 10^9

Per (device, reading_id) only the highest revision is adopted.  A delete at
the highest revision removes the point from accounting but stays on record,
so an older file cannot resurrect it.  Within one file and against the
ledger, the highest revision of an ID must hold a single message: different
messages at the highest revision reject the whole batch, identical replays
are harmless, and lower well-formed revisions take no part in the decision.
A put may move an existing ID to a new second; two effective IDs of one
device may never occupy the same second — checked on the post-batch state,
so one batch may move or retract a placeholder and reuse its second.  Bad
JSONL, a bad message, a highest-version conflict, or a final time conflict
rejects the whole file: no half-applied readings, no half-updated ledger,
and a non-zero exit.  Every apply is one SQLite transaction, so a process
reopening the database sees either the complete pre-commit or the complete
post-commit state.

Shifts are fixed: every 28,800 seconds from the UTC epoch.  Effective
readings of one device, sorted by time, form metering intervals between
adjacent points only; [first, next) is left-closed right-open.  Same epoch
and non-falling counter make a reliable difference.  A zero difference
confirms zero increment for every shift the interval touches, even across
shifts.  A reliable positive difference is credited to a shift only when
the whole interval lies inside that shift — never apportioned.  A
cross-shift positive difference, an epoch change, or a falling counter
makes every intersected second of the interval uncertain.  Time before the
first reading and after the last is not covered: nothing to confirm, and
no uncertain seconds either.  A reading exactly on a shift boundary ends
the previous interval and may start the next one; the previous interval
does not thereby cross the shift.

`query DEVICE START END` takes two shift starts (multiples of 28800,
START < END) and prints one JSON line per shift in [START, END):

    device, start, end,
    confirmed_increment  increments attributable to this shift
    covered_seconds      seconds of the shift bracketed by adjacent readings
    uncertain_seconds    covered seconds that cannot be attributed
    exact                true only when fully covered with nothing uncertain

Empty shifts are emitted too; an unknown device reports covered_seconds 0
and exact false — never a full-shift, exact, zero-output shift.

Storage is incremental.  Applying one batch touches only the intervals
adjacent to the old and new positions of the readings it changes; an
interval spanning years of empty shifts is stored as two edge rows plus
its endpoints, never expanded per shift.  A query costs O(log n) per
requested shift: it never replays import files, never scans a device's
history, and never walks dense readings inside a shift one by one.
Import files and query results are streamed; oversized scratch data lives
in temporary tables on disk.

`settle` reconciles the ledger with a job-assignment timeline and exports
a settlement manifest as JSONL:

    python3 -I shift_ledger.py settle DB --from FROM --to TO
        --assign ASSIGNMENTS [-o OUTPUT] DEVICE [DEVICE ...]

The assignment file holds one JSON object per line: device (non-empty
string), job (non-empty string), start/end (integer UTC seconds,
0 <= start < end <= 2^53-1).  [start, end) is left-closed right-open, and
several simultaneously active rows for the same device and job count as
one job.  Line order is irrelevant.  Every line is validated, even lines
naming devices not being settled; any invalid line rejects the whole
export and leaves a previously delivered manifest untouched.

For each device (naming one twice settles it once), metering intervals
are the adjacent pairs of effective readings, exactly as above.  The
export emits every interval whose right reading time falls in
[FROM, TO): the left reading may lie before FROM and is the true previous
reading, never the range start, and no interval is ever cut to fit the
range — adjacent export windows agree on the same intervals.  For an
interval [t1, t2) the assignment facts are judged over its whole span,
including any part before FROM.  One JSONL line per emitted interval,
ordered by device (Unicode code points) then right-end second:

    device, start, end,
    left_id, left_revision, right_id, right_revision,
    status, job, amount

status is one of:

    allocated     reliable positive difference; every second of the
                  interval had exactly the same single job available.
                  job names it; amount is the full difference, never
                  apportioned.
    overlap       some second had two different jobs active.
    uncovered     no overlapping second, but some second had no job.
    handoff       every second had exactly one job, but not always the
                  same one.
    unmeasurable  epoch change or falling counter; job and amount are
                  null whatever the assignments say.

overlap / uncovered / handoff keep one detail line with the full positive
difference and job null.  A reliable zero difference settles nothing and
emits no line.

The whole export reads one committed snapshot of the ledger: an apply
committing while it runs cannot splice two ledger versions into one
manifest.  Only the window's readings plus one predecessor per device
are fetched; assignments are streamed into indexed temporary tables and
swept once per device — never expanded per second or shift, never
rescanned per interval, and never joined into an interval-by-assignment
cross product.  Python memory holds neither the full assignment table
nor the selected readings nor the manifest; scratch data lives in
temporary tables on disk.  OUTPUT is written atomically (temp file +
rename); any failure leaves the previous manifest in place.

Offline replica sync (`sync-init`, `sync-pack`, `sync-receive`)
================================================================

A branch office keeps its own SQLite copy of the ledger but has no
network access to the central database.  Synchronisation travels by
handed-over files only: the centre publishes an increment *pack*, the
branch feeds it to its copy.  There is no service and no two-way merge;
a replica never invents changes and cannot act as a centre.

The centre's synchronisation identity is generated once, persisted in
the database, and never replaced afterwards — later `apply` and export
runs on that database stay bound to it.  Separate centres generate
independent identities.  Synchronisation positions start at 0 (a blank
replica).  When sync is first enabled on a ledger that already holds
data, every current highest version (tombstones included) becomes
version 1; an empty ledger starts at 0 and needs no replay of the
original inputs.  From then on, every `apply` whose winners change at
least one highest version advances the position by one — a revision
bump that leaves the reading values untouched still advances, while a
failed batch, an old-only batch, or an identical replay does not.  The
position commits in the same transaction as that apply's result.

`sync-pack DB --base N [-o PACK]` publishes a pack from centre DB.
The pack names the source identity, the base position N and the end
position, which is one complete committed version seen while packing.
For every (device, reading_id) that changed at least once between the
two positions the pack carries exactly one row: that identity's
highest version at the end position, including a delete tombstone.
Identities that never changed are not carried.  Base 0 means "sync
from blank" and carries the whole current state.  A base beyond the
current position is an error; a base equal to the current position
reports "no update" and leaves the target file untouched.  An apply
committing while a pack is built may land in this pack or the next,
but one pack's end position and its rows always belong to the same
committed snapshot.  The changed-identity set is held in a durable
log table updated inside each apply transaction, so a small daily
pack scans only changed identities, never the untouched history, and
a pack's business rows grow with the number of changed identities,
not with how often each was modified.

`sync-receive DB PACK` applies a pack to a branch copy.  The first
pack may only enter an empty, unbound database and must have base 0;
it binds the copy to the pack's source identity.  Afterwards every
pack must come from that same identity and its base must equal the
copy's current position — a missing pack (gap), an out-of-order pack,
or a foreign source is rejected without any guessed merge.  Replaying
the byte-identical file that was just accepted is a successful
no-op (matched by file digest); a different file ending at the same
position is not a replay and is refused.  A replica cannot run plain
`apply` (so it cannot alter readings) and cannot pack.  Rows are
loaded and judged as the pack's *final* state, exactly like one big
batch: per-row intermediate seconds never reject a state whose
post-pack readings are legal.  Readings, shift summaries, the source
binding and the sync position take effect together, in one
transaction, only after the whole pack has been verified; after a bad
pack or a failure, reopening the database shows the old state and the
original good pack can still be re-fed.  Success reports the copy's
new current position, which is the base for the next pack.  Replicas
keep using the unchanged `query` and `settle` (including the reading
evidence revisions in `settle` output); the assignment table is not
part of a pack and the accounting rules never change.

Pack file format (binary, streamed; big packs spill to disk via temp
tables and never load whole into memory):

    magic      8 bytes  b"SLPACK01"
    frame      u32 LE length L, L bytes UTF-8 JSON header, then...
    frame      repeated zero or more times: u32 LE length L, L bytes
               UTF-8 JSON row
    frame      one zero-length frame (L = 0) marks the end of rows
    digest     32 raw bytes: SHA-256 over every byte preceding it
               (magic, header frame, row frames, the zero frame)
    then the file must end immediately

The header JSON is {"format":1,"source":ID,"base":N,"end":M} with
N >= 0, M >= N; each row JSON is {"device":...,"reading_id":...,
"revision":...,"op":"put"|"delete", and for puts "epoch","time",
"counter"}.  The length-prefixed framing makes truncation detectable,
the digest detects any altered content, and the required immediate EOF
detects extra trailing bytes (transport errors only; no signatures).

Usage:
    python3 -I shift_ledger.py apply DB JSONL
    python3 -I shift_ledger.py query DB DEVICE START END [-o OUTPUT]
    python3 -I shift_ledger.py settle DB --from FROM --to TO
        --assign ASSIGNMENTS [-o OUTPUT] DEVICE [DEVICE ...]
    python3 -I shift_ledger.py sync-init DB
    python3 -I shift_ledger.py sync-pack DB --base N [-o PACK]
    python3 -I shift_ledger.py sync-receive DB PACK

Exit status:
    0  success
    2  invalid input (bad JSONL/message, version or time conflict, bad
       query range, bad settle range, bad assignment JSONL, bad or
       inapplicable sync pack, wrong sync position or source)
    1  I/O or unexpected failure
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sqlite3
import sys
import tempfile
import traceback
import uuid

SHIFT = 28800
MAX_REVISION = (1 << 63) - 1
MAX_TIME = (1 << 53) - 1
MAX_COUNTER = 10 ** 9
MAX_BOUND = (1 << 63) - 1

# Binary synchronisation pack framing (see module docstring).
PACK_MAGIC = b"SLPACK01"
PACK_FORMAT = 1
PACK_DIGEST_SIZE = hashlib.sha256().digest_size
# Frames hold one JSON object; readings are bounded by the message
# limits above, so a generous fixed cap suffices to reject corrupt
# lengths without buffering unbounded data.
PACK_MAX_FRAME = 1 << 20


class BatchError(Exception):
    """The input file or its messages are invalid; nothing was applied."""


class QueryError(Exception):
    """The query arguments are invalid."""


class LedgerIOError(Exception):
    """The database or an output file could not be opened or written."""


class AssignmentError(Exception):
    """The assignment JSONL given to `settle` is invalid."""


class SyncError(Exception):
    """A sync pack or a sync-state operation is invalid; nothing changed.

    Like BatchError this means invalid input (exit status 2), not an
    I/O failure.
    """


def _reject_constant(name):
    # Strict JSON has no NaN/Infinity.
    raise ValueError(f"non-finite JSON number {name!r} is not allowed")


def _is_int(value):
    return type(value) is int


def make_content(op, epoch, time, counter):
    """Canonical comparison form of one message's effective payload."""
    return json.dumps([op, epoch, time, counter],
                      ensure_ascii=True, separators=(",", ":"))


# ---------------------------------------------------------------------------
# Message validation
# ---------------------------------------------------------------------------

def validate_message(obj, where):
    """Return (device, reading_id, revision, op, epoch, time, counter,
    content) or raise BatchError.  epoch/time/counter are None for deletes.
    """
    if not isinstance(obj, dict):
        raise BatchError(f"{where}: message must be a JSON object")
    device = obj.get("device")
    if not isinstance(device, str) or not device:
        raise BatchError(f"{where}: 'device' must be a non-empty string")
    reading_id = obj.get("reading_id")
    if not isinstance(reading_id, str) or not reading_id:
        raise BatchError(f"{where}: 'reading_id' must be a non-empty string")
    revision = obj.get("revision")
    if not _is_int(revision) or not 1 <= revision <= MAX_REVISION:
        raise BatchError(
            f"{where}: 'revision' must be an integer in 1..{MAX_REVISION}")
    op = obj.get("op")
    if op == "put":
        epoch = obj.get("epoch")
        if not isinstance(epoch, str) or not epoch:
            raise BatchError(f"{where}: 'epoch' must be a non-empty string")
        time = obj.get("time")
        if not _is_int(time) or not 0 <= time <= MAX_TIME:
            raise BatchError(
                f"{where}: 'time' must be an integer in 0..{MAX_TIME}")
        counter = obj.get("counter")
        if not _is_int(counter) or not 0 <= counter <= MAX_COUNTER:
            raise BatchError(
                f"{where}: 'counter' must be an integer in 0..{MAX_COUNTER}")
    elif op == "delete":
        epoch = time = counter = None
    else:
        raise BatchError(f"{where}: 'op' must be 'put' or 'delete'")
    return (device, reading_id, revision, op, epoch, time, counter,
            make_content(op, epoch, time, counter))


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
    device     TEXT NOT NULL,
    reading_id TEXT NOT NULL,
    revision   INTEGER NOT NULL,
    op         TEXT NOT NULL,
    epoch      TEXT,
    time       INTEGER,
    counter    INTEGER,
    PRIMARY KEY (device, reading_id)
);
-- Effective points of one device ordered by second (puts only).
CREATE INDEX IF NOT EXISTS readings_by_time
    ON readings (device, time) WHERE op = 'put';
-- Reliable positive increments fully inside one shift, summed per shift.
CREATE TABLE IF NOT EXISTS confirmed (
    device    TEXT NOT NULL,
    shift     INTEGER NOT NULL,
    increment INTEGER NOT NULL,
    PRIMARY KEY (device, shift)
);
-- Uncertain seconds contributed by interval ends that fall inside one
-- shift.  Whole shifts spanned by an uncertain interval are NOT stored
-- here; they are derived from the interval endpoints at query time.
CREATE TABLE IF NOT EXISTS uncertain_edge (
    device  TEXT NOT NULL,
    shift   INTEGER NOT NULL,
    seconds INTEGER NOT NULL,
    PRIMARY KEY (device, shift)
);
-- Centre role: one row once sync-init has run.  The identity is fixed
-- for the life of the database; position is the committed version
-- number of the current readings state.
CREATE TABLE IF NOT EXISTS sync_center (
    id       INTEGER PRIMARY KEY CHECK (id = 1),
    identity TEXT NOT NULL,
    position INTEGER NOT NULL
);
-- Replica role: one row once the first base-0 pack was received.  The
-- source binding and position only move inside a receive transaction.
CREATE TABLE IF NOT EXISTS sync_replica (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    source       TEXT NOT NULL,
    position     INTEGER NOT NULL,
    last_digest  TEXT NOT NULL
);
-- Centre-only durable log of every identity whose highest version has
-- ever changed, with the position at which its latest change landed.
-- One row per identity regardless of how many times it changed, so an
-- incremental pack between two positions scans only changed
-- identities — never the untouched history.
CREATE TABLE IF NOT EXISTS sync_log (
    device       TEXT NOT NULL,
    reading_id   TEXT NOT NULL,
    last_version INTEGER NOT NULL,
    PRIMARY KEY (device, reading_id)
) WITHOUT ROWID;
-- Range access by version: an incremental pack looks up only the
-- identities changed after its base, never walks the unchanged ones.
CREATE INDEX IF NOT EXISTS sync_log_versions
    ON sync_log (last_version);
"""


def connect(db_path):
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA busy_timeout = 10000")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA temp_store = FILE")
        conn.executescript(SCHEMA)
    except sqlite3.Error as exc:
        raise LedgerIOError(f"cannot open ledger {db_path!r}: {exc}") from exc
    return conn


# ---------------------------------------------------------------------------
# Sync role helpers
# ---------------------------------------------------------------------------

def center_state(conn):
    """Return (identity, position) of a centre database, else None."""
    return conn.execute(
        "SELECT identity, position FROM sync_center WHERE id = 1"
    ).fetchone()


def replica_state(conn):
    """Return (source, position, last_digest) of a replica, else None."""
    return conn.execute(
        "SELECT source, position, last_digest FROM sync_replica WHERE id = 1"
    ).fetchone()


# ---------------------------------------------------------------------------
# Interval bookkeeping
# ---------------------------------------------------------------------------

def interval_kind(e1, c1, t1, e2, c2, t2):
    """Classify the metering interval [t1, t2).

    Returns 'zero' (reliable zero difference), 'counted' (reliable positive
    difference inside one shift), or 'uncertain'.
    """
    if e1 == e2 and c2 >= c1:
        if c2 == c1:
            return "zero"
        if t1 // SHIFT == (t2 - 1) // SHIFT:
            return "counted"
    return "uncertain"


def _add_summary(conn, table, column, device, shift, delta):
    conn.execute(
        f"INSERT INTO {table} (device, shift, {column}) VALUES (?, ?, ?) "
        f"ON CONFLICT(device, shift) DO UPDATE SET {column} = {column} "
        f"+ excluded.{column}",
        (device, shift, delta))
    conn.execute(
        f"DELETE FROM {table} WHERE device = ? AND shift = ? AND {column} = 0",
        (device, shift))


def _interval_delta(conn, device, t1, e1, c1, t2, e2, c2, sign):
    """Apply (+1) or revoke (-1) interval [t1, t2)'s summary contributions."""
    kind = interval_kind(e1, c1, t1, e2, c2, t2)
    if kind == "zero":
        return
    if kind == "counted":
        _add_summary(conn, "confirmed", "increment", device,
                     t1 // SHIFT, sign * (c2 - c1))
        return
    s1 = t1 // SHIFT
    s2 = (t2 - 1) // SHIFT
    if s1 == s2:
        _add_summary(conn, "uncertain_edge", "seconds", device,
                     s1, sign * (t2 - t1))
    else:
        # Only the two edge shifts are stored; the shifts in between are
        # fully spanned and recovered at query time from the endpoints.
        _add_summary(conn, "uncertain_edge", "seconds", device,
                     s1, sign * ((s1 + 1) * SHIFT - t1))
        _add_summary(conn, "uncertain_edge", "seconds", device,
                     s2, sign * (t2 - s2 * SHIFT))


def _pred(conn, device, time):
    return conn.execute(
        "SELECT time, epoch, counter FROM readings "
        "WHERE device = ? AND op = 'put' AND time < ? "
        "ORDER BY time DESC LIMIT 1",
        (device, time)).fetchone()


def _succ(conn, device, time):
    return conn.execute(
        "SELECT time, epoch, counter FROM readings "
        "WHERE device = ? AND op = 'put' AND time > ? "
        "ORDER BY time ASC LIMIT 1",
        (device, time)).fetchone()


def _put_at(conn, device, time):
    return conn.execute(
        "SELECT 1 FROM readings WHERE device = ? AND op = 'put' AND time = ?",
        (device, time)).fetchone()


# ---------------------------------------------------------------------------
# Apply
# ---------------------------------------------------------------------------

def load_batch(conn, input_path):
    """Stream the JSONL file into the temp batch table, validating messages."""
    conn.execute("DROP TABLE IF EXISTS temp.batch")
    conn.execute(
        "CREATE TEMP TABLE batch ("
        "device TEXT NOT NULL, reading_id TEXT NOT NULL, "
        "revision INTEGER NOT NULL, op TEXT NOT NULL, "
        "epoch TEXT, time INTEGER, counter INTEGER, content TEXT NOT NULL)")
    close = False
    if input_path == "-":
        fh = sys.stdin
    else:
        try:
            fh = open(input_path, "r", encoding="utf-8")
        except OSError as exc:
            raise BatchError(
                f"cannot read input {input_path!r}: "
                f"{exc.strerror or exc}") from exc
        close = True
    count = 0
    try:
        insert = ("INSERT INTO batch (device, reading_id, revision, op, "
                  "epoch, time, counter, content) VALUES (?,?,?,?,?,?,?,?)")
        lineno = 0
        try:
            for line in fh:
                lineno += 1
                text = line.strip()
                if not text:
                    continue
                try:
                    row = json.loads(text, parse_constant=_reject_constant)
                except (ValueError, RecursionError) as exc:
                    raise BatchError(f"line {lineno}: invalid JSON: {exc}")
                if not isinstance(row, dict):
                    raise BatchError(
                        f"line {lineno}: line must be a JSON object")
                rtype = row.get("type")
                if not isinstance(rtype, str):
                    raise BatchError(
                        f"line {lineno}: 'type' must be a string")
                if rtype != "record":
                    # missing / conflict / invalid / boundary_unknown / any
                    # other line type: no reading is taken from it.
                    continue
                if "object" not in row:
                    raise BatchError(
                        f"line {lineno}: record line is missing 'object'")
                conn.execute(insert, validate_message(
                    row["object"], f"line {lineno}"))
                count += 1
        except UnicodeDecodeError as exc:
            raise BatchError(f"input is not valid UTF-8: {exc}") from exc
        except OSError as exc:
            raise BatchError(f"cannot read input: {exc}") from exc
    finally:
        if close:
            fh.close()
    return count


def _iv_insert(conn, table, device, t1, e1, c1, t2, e2, c2):
    conn.execute(
        f"INSERT OR IGNORE INTO {table} "
        f"(device, t1, e1, c1, t2, e2, c2) VALUES (?,?,?,?,?,?,?)",
        (device, t1, e1, c1, t2, e2, c2))


def _iv_apply(conn, table, sign):
    for device, t1, e1, c1, t2, e2, c2 in conn.execute(
            f"SELECT device, t1, e1, c1, t2, e2, c2 FROM {table}"):
        _interval_delta(conn, device, t1, e1, c1, t2, e2, c2, sign)


def apply_winners(conn):
    """Adopt every batch message whose revision beats the ledger's.

    Interval summaries are updated by snapshot, not by per-row mutation:
    the intervals to revoke are collected from the pre-batch table and the
    intervals to add from the post-batch table, each set deduplicated, so
    batches that move several points at once (swaps, a point landing on a
    vacated second, a point moving into an untouched interval) stay exact.
    """
    conn.execute("CREATE INDEX IF NOT EXISTS temp.batch_by_id "
                 "ON batch (device, reading_id, revision)")
    conn.execute("DROP TABLE IF EXISTS temp.chosen")
    conn.execute(
        "CREATE TEMP TABLE chosen ("
        "device TEXT NOT NULL, reading_id TEXT NOT NULL, "
        "revision INTEGER NOT NULL, op TEXT NOT NULL, "
        "epoch TEXT, time INTEGER, counter INTEGER, "
        "old_op TEXT, old_epoch TEXT, old_time INTEGER, old_counter INTEGER,"
        " same_point INTEGER NOT NULL, "
        "PRIMARY KEY (device, reading_id))")
    conn.execute("DROP TABLE IF EXISTS temp.touched")
    conn.execute("CREATE TEMP TABLE touched ("
                 "device TEXT NOT NULL, time INTEGER NOT NULL, "
                 "PRIMARY KEY (device, time))")
    for name in ("old_iv", "new_iv"):
        conn.execute(f"DROP TABLE IF EXISTS temp.{name}")
        conn.execute(
            f"CREATE TEMP TABLE {name} ("
            "device TEXT NOT NULL, t1 INTEGER NOT NULL, e1 TEXT, "
            "c1 INTEGER, t2 INTEGER NOT NULL, e2 TEXT, c2 INTEGER, "
            "PRIMARY KEY (device, t1, t2))")

    # Phase 1: decide every winner against the ledger; change nothing yet.
    # The highest revision of an ID — across this file and the ledger —
    # must hold a single message; lower well-formed revisions take no part.
    # The ordered scan groups one ID's messages together, so the whole
    # file is resolved in one streaming pass without a join.
    lookup = ("SELECT revision, op, epoch, time, counter FROM readings "
              "WHERE device = ? AND reading_id = ?")
    chosen_insert = ("INSERT INTO chosen (device, reading_id, revision, op,"
                     " epoch, time, counter, old_op, old_epoch, old_time,"
                     " old_counter, same_point) VALUES (?,?,?,?,?,?,?,?,?,?,"
                     "?,?)")
    ordered = conn.execute(
        "SELECT device, reading_id, revision, op, epoch, time, counter, "
        "content FROM batch ORDER BY device, reading_id, revision")
    group_key = None
    group_rev = None
    group_contents = None
    group_row = None

    def flush_group():
        if group_key is None:
            return
        device, reading_id = group_key
        rev = group_rev
        old = conn.execute(lookup, (device, reading_id)).fetchone()
        if old is not None and old[0] > rev:
            return  # the ledger's higher version stands; the file's
            # lower versions take no part in the decision
        if len(group_contents) > 1:
            raise BatchError(
                f"conflicting messages at the highest revision {rev} of "
                f"reading {reading_id!r} (device {device!r})")
        if old is not None and old[0] == rev:
            if make_content(old[1], old[2], old[3], old[4]) \
                    != next(iter(group_contents)):
                raise BatchError(
                    f"conflicting messages at the highest revision {rev} "
                    f"of reading {reading_id!r} (device {device!r})")
            return  # identical replay of the current version
        _, _, _, op, epoch, time, counter, _ = group_row
        old_op = old[1] if old is not None else None
        same_point = 1 if (old_op == "put" and op == "put"
                           and old[2] == epoch and old[3] == time
                           and old[4] == counter) else 0
        conn.execute(chosen_insert,
                     (device, reading_id, rev, op, epoch, time, counter,
                      old_op,
                      old[2] if old is not None else None,
                      old[3] if old is not None else None,
                      old[4] if old is not None else None,
                      same_point))

    for row in ordered:
        key = (row[0], row[1])
        if key != group_key:
            flush_group()
            group_key = key
            group_rev = row[2]
            group_contents = {row[7]}
            group_row = row
        elif row[2] > group_rev:
            group_rev = row[2]
            group_contents = {row[7]}
            group_row = row
        elif row[2] == group_rev:
            group_contents.add(row[7])
    flush_group()

    # Phase A: intervals to revoke, read from the pre-batch table.
    for device, t, e, c in conn.execute(
            "SELECT device, old_time, old_epoch, old_counter FROM chosen "
            "WHERE old_op = 'put' AND same_point = 0"):
        pred = _pred(conn, device, t)
        succ = _succ(conn, device, t)
        if pred is not None:
            _iv_insert(conn, "old_iv", device, pred[0], pred[1], pred[2],
                       t, e, c)
        if succ is not None:
            _iv_insert(conn, "old_iv", device, t, e, c,
                       succ[0], succ[1], succ[2])
    # A point landing on a fresh second splits the initial interval that
    # contains it; that interval has no changed endpoint, so collect it too.
    for device, t in conn.execute(
            "SELECT device, time FROM chosen "
            "WHERE op = 'put' AND same_point = 0"):
        if _put_at(conn, device, t) is not None:
            continue  # an old point sits there; its own intervals are listed
        pred = _pred(conn, device, t)
        succ = _succ(conn, device, t)
        if pred is not None and succ is not None:
            _iv_insert(conn, "old_iv", device, pred[0], pred[1], pred[2],
                       succ[0], succ[1], succ[2])
    _iv_apply(conn, "old_iv", -1)

    # Phase B: adopt the new current versions.
    upsert = ("INSERT OR REPLACE INTO readings "
              "(device, reading_id, revision, op, epoch, time, counter) "
              "VALUES (?,?,?,?,?,?,?)")
    applied = 0
    for device, reading_id, rev, op, epoch, time, counter, same_point in \
            conn.execute(
                "SELECT device, reading_id, revision, op, epoch, time, "
                "counter, same_point FROM chosen"):
        conn.execute(upsert, (device, reading_id, rev, op,
                              epoch, time, counter))
        if op == "put" and not same_point:
            conn.execute("INSERT OR IGNORE INTO touched (device, time) "
                         "VALUES (?,?)", (device, time))
        applied += 1

    # Phase C: intervals to add, read from the post-batch table.
    for device, t, e, c in conn.execute(
            "SELECT device, time, epoch, counter FROM chosen "
            "WHERE op = 'put' AND same_point = 0"):
        pred = _pred(conn, device, t)
        succ = _succ(conn, device, t)
        if pred is not None:
            _iv_insert(conn, "new_iv", device, pred[0], pred[1], pred[2],
                       t, e, c)
        if succ is not None:
            _iv_insert(conn, "new_iv", device, t, e, c,
                       succ[0], succ[1], succ[2])
    # A vacated second merges its two post-batch neighbours into one
    # interval — unless some other point of this batch now occupies it.
    for device, t in conn.execute(
            "SELECT device, old_time FROM chosen "
            "WHERE old_op = 'put' AND same_point = 0"):
        if _put_at(conn, device, t) is not None:
            continue
        pred = _pred(conn, device, t)
        succ = _succ(conn, device, t)
        if pred is not None and succ is not None:
            _iv_insert(conn, "new_iv", device, pred[0], pred[1], pred[2],
                       succ[0], succ[1], succ[2])
    _iv_apply(conn, "new_iv", +1)
    return applied


def check_time_conflicts(conn):
    """No two effective IDs of one device may share a second (post-batch).

    Only seconds this batch placed a point on can be affected, so the
    check stays proportional to the batch, not to the device's history.
    """
    count = ("SELECT COUNT(*) FROM readings "
             "WHERE device = ? AND time = ? AND op = 'put'")
    for device, t in conn.execute("SELECT device, time FROM touched"):
        row = conn.execute(count, (device, t)).fetchone()
        if row[0] > 1:
            raise BatchError(
                f"device {device!r} has two effective readings at time {t}")


def apply_batch(db_path, input_path):
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        if replica_state(conn) is not None:
            raise SyncError(
                "this database is a sync replica: readings can only be "
                "changed via sync-receive, not apply")
        received = load_batch(conn, input_path)
        applied = apply_winners(conn)
        check_time_conflicts(conn)
        center = center_state(conn)
        if center is not None and applied > 0:
            # A position advance and the results of this apply commit
            # together — same transaction, same visible version.  The
            # log keeps one row per changed identity at the new
            # position, no matter how often that identity changed.
            new_position = center[1] + 1
            conn.execute("UPDATE sync_center SET position = ? WHERE id = 1",
                         (new_position,))
            conn.executemany(
                "INSERT INTO sync_log (device, reading_id, last_version) "
                "VALUES (?, ?, ?) ON CONFLICT(device, reading_id) DO UPDATE "
                "SET last_version = excluded.last_version",
                conn.execute(
                    "SELECT device, reading_id, ? FROM chosen",
                    (new_position,)))
        conn.commit()
        return received, applied
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

def shift_rows(conn, device, start, end):
    """Yield one accounting dict per shift in [start, end)."""
    # MIN and MAX in separate statements: each resolves to a single index
    # seek, while a combined MIN()/MAX() would scan the device's history.
    row = conn.execute(
        "SELECT MIN(time) FROM readings "
        "WHERE device = ? AND op = 'put'", (device,)).fetchone()
    tmin = row[0] if row is not None else None
    row = conn.execute(
        "SELECT MAX(time) FROM readings "
        "WHERE device = ? AND op = 'put'", (device,)).fetchone()
    tmax = row[0] if row is not None else None
    get_confirmed = ("SELECT increment FROM confirmed "
                     "WHERE device = ? AND shift = ?")
    get_edge = ("SELECT seconds FROM uncertain_edge "
                "WHERE device = ? AND shift = ?")
    s = start
    while s < end:
        e = s + SHIFT
        shift = s // SHIFT
        row = conn.execute(get_confirmed, (device, shift)).fetchone()
        confirmed = row[0] if row is not None else 0
        row = conn.execute(get_edge, (device, shift)).fetchone()
        uncertain = row[0] if row is not None else 0
        # A single interval may span this whole shift; find the latest
        # point before the shift and check its successor.
        pred = _pred(conn, device, s)
        if pred is not None:
            succ = _succ(conn, device, pred[0])
            if (succ is not None and succ[0] > e
                    and interval_kind(pred[1], pred[2], pred[0],
                                      succ[1], succ[2], succ[0])
                    == "uncertain"):
                uncertain += SHIFT
        covered = 0
        if tmin is not None:
            covered = max(0, min(e, tmax) - max(s, tmin))
        yield {
            "device": device,
            "start": s,
            "end": e,
            "confirmed_increment": confirmed,
            "covered_seconds": covered,
            "uncertain_seconds": uncertain,
            "exact": covered == SHIFT and uncertain == 0,
        }
        s = e


class OutputWriter:
    """Line emitter; a real path is written atomically via temp + rename."""

    def __init__(self, target):
        self.target = target
        self.tmp_path = None
        self.fh = None
        self.committed = False
        if target == "-":
            self.fh = sys.stdout
            return
        directory = os.path.dirname(os.path.abspath(target))
        base = os.path.basename(target)
        try:
            fd, self.tmp_path = tempfile.mkstemp(
                prefix=f".{base}.", suffix=".tmp", dir=directory)
            self.fh = os.fdopen(fd, "w", encoding="utf-8", newline="\n")
        except OSError as exc:
            raise LedgerIOError(
                f"cannot create temporary output next to {target!r}: "
                f"{exc.strerror or exc}") from exc

    def line(self, obj):
        try:
            self.fh.write(json.dumps(obj, ensure_ascii=True,
                                     separators=(",", ":")))
            self.fh.write("\n")
        except OSError as exc:
            raise LedgerIOError(f"cannot write output: "
                                f"{exc.strerror or exc}") from exc

    def commit(self):
        if self.committed:
            return
        if self.tmp_path is None:
            try:
                self.fh.flush()
            except OSError as exc:
                raise LedgerIOError(f"cannot write output: "
                                    f"{exc.strerror or exc}") from exc
            self.committed = True
            return
        try:
            self.fh.flush()
            os.fsync(self.fh.fileno())
            self.fh.close()
            self.fh = None
            os.replace(self.tmp_path, self.target)
            self.tmp_path = None
        except OSError as exc:
            raise LedgerIOError(f"cannot finalise output {self.target!r}: "
                                f"{exc.strerror or exc}") from exc
        self.committed = True

    def abort(self):
        if self.committed:
            return
        if self.fh is not None and self.tmp_path is not None:
            try:
                self.fh.close()
            except OSError:
                pass
            self.fh = None
        if self.tmp_path is not None:
            try:
                os.unlink(self.tmp_path)
            except OSError:
                pass
            self.tmp_path = None


def run_query(db_path, device, start, end, output_path):
    for name, value in (("START", start), ("END", end)):
        if not 0 <= value <= MAX_BOUND:
            raise QueryError(f"{name} must be in 0..{MAX_BOUND}")
        if value % SHIFT != 0:
            raise QueryError(f"{name} must be a shift start "
                             f"(a multiple of {SHIFT})")
    if start >= end:
        raise QueryError("START must be smaller than END")
    conn = connect(db_path)
    writer = None
    try:
        writer = OutputWriter(output_path)
        for row in shift_rows(conn, device, start, end):
            writer.line(row)
        writer.commit()
    except BaseException:
        if writer is not None:
            writer.abort()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Settle export
# ---------------------------------------------------------------------------

def validate_assignment(obj, where):
    """Return (device, job, start, end) or raise AssignmentError."""
    if not isinstance(obj, dict):
        raise AssignmentError(f"{where}: line must be a JSON object")
    device = obj.get("device")
    if not isinstance(device, str) or not device:
        raise AssignmentError(
            f"{where}: 'device' must be a non-empty string")
    job = obj.get("job")
    if not isinstance(job, str) or not job:
        raise AssignmentError(f"{where}: 'job' must be a non-empty string")
    start = obj.get("start")
    if not _is_int(start) or start < 0:
        raise AssignmentError(f"{where}: 'start' must be an integer >= 0")
    end = obj.get("end")
    if not _is_int(end) or end > MAX_TIME:
        raise AssignmentError(
            f"{where}: 'end' must be an integer <= {MAX_TIME}")
    if start >= end:
        raise AssignmentError(f"{where}: 'start' must be smaller than 'end'")
    return device, job, start, end


def load_assignments(conn, input_path, wanted):
    """Stream the assignment JSONL into a temp table for wanted devices.

    Every line is validated — including lines for devices not being
    settled — but only the wanted devices' rows are kept.  Returns
    (received, kept).
    """
    conn.execute("DROP TABLE IF EXISTS temp.assign")
    conn.execute(
        "CREATE TEMP TABLE assign ("
        "device TEXT NOT NULL, job TEXT NOT NULL, "
        "start INTEGER NOT NULL, end INTEGER NOT NULL)")
    close = False
    if input_path == "-":
        fh = sys.stdin
    else:
        try:
            fh = open(input_path, "r", encoding="utf-8")
        except OSError as exc:
            raise AssignmentError(
                f"cannot read assignments {input_path!r}: "
                f"{exc.strerror or exc}") from exc
        close = True
    received = 0
    kept = 0
    try:
        insert = ("INSERT INTO assign (device, job, start, end) "
                  "VALUES (?,?,?,?)")
        lineno = 0
        try:
            for line in fh:
                lineno += 1
                text = line.strip()
                if not text:
                    continue
                try:
                    row = json.loads(text, parse_constant=_reject_constant)
                except (ValueError, RecursionError) as exc:
                    raise AssignmentError(
                        f"line {lineno}: invalid JSON: {exc}")
                device, job, start, end = validate_assignment(
                    row, f"line {lineno}")
                received += 1
                if device in wanted:
                    conn.execute(insert, (device, job, start, end))
                    kept += 1
        except UnicodeDecodeError as exc:
            raise AssignmentError(
                f"assignments are not valid UTF-8: {exc}") from exc
        except OSError as exc:
            raise AssignmentError(
                f"cannot read assignments: {exc}") from exc
    finally:
        if close:
            fh.close()
    conn.execute("CREATE INDEX IF NOT EXISTS temp.assign_by_start "
                 "ON assign (device, start, end, job)")
    conn.execute("CREATE INDEX IF NOT EXISTS temp.assign_by_end "
                 "ON assign (device, end, start, job)")
    return received, kept


class AssignmentSweep:
    """One device's assignment coverage, swept left to right.

    The live set (job -> number of concurrent rows) sits in the temp
    table `active`, so nested assignments never pile up in Python
    memory; several concurrent rows of one job still count as one job.
    Add/remove events stream lazily from the two indexes — positions
    beyond the last classified interval are never read.
    """

    def __init__(self, conn, device, t0):
        self.conn = conn
        self.distinct = 0
        conn.execute("DELETE FROM active")
        for job, n in conn.execute(
                "SELECT job, COUNT(*) FROM assign "
                "WHERE device = ? AND start <= ? AND end > ? GROUP BY job",
                (device, t0, t0)):
            conn.execute("INSERT INTO active (job, n) VALUES (?, ?)",
                         (job, n))
            self.distinct += 1
        self.adds = conn.execute(
            "SELECT start, job FROM assign "
            "WHERE device = ? AND start > ? ORDER BY start", (device, t0))
        self.rems = conn.execute(
            "SELECT end, job FROM assign "
            "WHERE device = ? AND end > ? ORDER BY end", (device, t0))
        self.next_add = next(self.adds, None)
        self.next_rem = next(self.rems, None)

    def position(self):
        """Next event position, or None when both streams run dry."""
        pos = None
        if self.next_add is not None:
            pos = self.next_add[0]
        if self.next_rem is not None and \
                (pos is None or self.next_rem[0] < pos):
            pos = self.next_rem[0]
        return pos

    def _add(self, job):
        cur = self.conn.execute(
            "UPDATE active SET n = n + 1 WHERE job = ?", (job,))
        if cur.rowcount == 0:
            self.conn.execute(
                "INSERT INTO active (job, n) VALUES (?, 1)", (job,))
            self.distinct += 1

    def _remove(self, job):
        self.conn.execute(
            "UPDATE active SET n = n - 1 WHERE job = ?", (job,))
        cur = self.conn.execute(
            "DELETE FROM active WHERE job = ? AND n <= 0", (job,))
        if cur.rowcount:
            self.distinct -= 1

    def advance_to(self, pos):
        """Apply every event at `pos` (the position() just reported)."""
        while self.next_add is not None and self.next_add[0] == pos:
            self._add(self.next_add[1])
            self.next_add = next(self.adds, None)
        while self.next_rem is not None and self.next_rem[0] == pos:
            self._remove(self.next_rem[1])
            self.next_rem = next(self.rems, None)


class IntervalStats:
    """What the sweep saw of one metering interval, second by second."""

    __slots__ = ("overlap", "min_distinct", "first_job", "changed")

    def __init__(self):
        self.overlap = False       # some second had 2+ different jobs
        self.min_distinct = None   # fewest jobs active on any second
        self.first_job = None      # the single job of the first 1-job second
        self.changed = False       # a later 1-job second had another job


def sweep_interval(sweep, stats, lo, hi):
    """Fold the assignment coverage of [lo, hi) into stats.

    Only event positions are visited; seconds between two events share
    one coverage state, so nothing is ever expanded per second.  Events
    at positions <= lo are caught up first (they belong to intervals
    that needed no classification of their own).
    """
    pos = sweep.position()
    while pos is not None and pos <= lo:
        sweep.advance_to(pos)
        pos = sweep.position()
    _note(sweep, stats)
    while pos is not None and pos < hi:
        sweep.advance_to(pos)
        _note(sweep, stats)
        pos = sweep.position()


def _note(sweep, stats):
    if sweep.distinct >= 2:
        stats.overlap = True
        return  # one overlapping second already decides the interval
    if stats.min_distinct is None or sweep.distinct < stats.min_distinct:
        stats.min_distinct = sweep.distinct
    if sweep.distinct == 1 and not stats.changed:
        job = sweep.conn.execute("SELECT job FROM active").fetchone()[0]
        if stats.first_job is None:
            stats.first_job = job
        elif stats.first_job != job:
            stats.changed = True


def classify_interval(stats):
    """Map one interval's coverage stats to (status, job)."""
    if stats.overlap:
        return "overlap", None
    if stats.min_distinct == 0:
        return "uncovered", None
    if stats.changed:
        return "handoff", None
    return "allocated", stats.first_job


def emit_settle(writer, device, left, right, status, job, amount):
    writer.line({
        "device": device,
        "start": left[2],
        "end": right[2],
        "left_id": left[0],
        "left_revision": left[1],
        "right_id": right[0],
        "right_revision": right[1],
        "status": status,
        "job": job,
        "amount": amount,
    })


def settle_device(conn, device, t_from, t_to, writer):
    """Emit one manifest line per interval of `device` whose right
    reading time falls in [t_from, t_to).  Returns the line count.

    Reads only the window's readings plus the single predecessor that
    may open the first interval; the device's history outside the
    window is never scanned.
    """
    pred = conn.execute(
        "SELECT reading_id, revision, time, epoch, counter FROM readings "
        "WHERE device = ? AND op = 'put' AND time < ? "
        "ORDER BY time DESC LIMIT 1", (device, t_from)).fetchone()
    cursor = conn.execute(
        "SELECT reading_id, revision, time, epoch, counter FROM readings "
        "WHERE device = ? AND op = 'put' AND time >= ? AND time < ? "
        "ORDER BY time", (device, t_from, t_to))
    row = next(cursor, None)
    if row is None:
        return 0
    prev = pred
    if prev is None:
        prev = row  # the device's earliest reading ends no interval
        row = next(cursor, None)
    sweep = None
    emitted = 0
    while row is not None:
        lo, hi = prev[2], row[2]
        e1, c1 = prev[3], prev[4]
        e2, c2 = row[3], row[4]
        if e1 != e2 or c2 < c1:
            # The readings themselves are unreliable; no assignment
            # fact may paper over that.
            emit_settle(writer, device, prev, row,
                        "unmeasurable", None, None)
            emitted += 1
        elif c2 > c1:
            if sweep is None:
                sweep = AssignmentSweep(conn, device, lo)
            stats = IntervalStats()
            sweep_interval(sweep, stats, lo, hi)
            status, job = classify_interval(stats)
            emit_settle(writer, device, prev, row, status, job, c2 - c1)
            emitted += 1
        # else: a reliable zero difference settles nothing — no line.
        prev = row
        row = next(cursor, None)
    return emitted


def run_settle(db_path, devices, t_from, t_to, assign_path, output_path):
    if not 0 <= t_from < t_to <= MAX_TIME + 1:
        raise QueryError(
            f"range must satisfy 0 <= FROM < TO <= {MAX_TIME + 1}")
    wanted = set()
    for device in devices:
        if not isinstance(device, str) or not device:
            raise QueryError("DEVICE must be a non-empty string")
        wanted.add(device)
    conn = connect(db_path)
    writer = None
    try:
        conn.execute("BEGIN")
        # Pin one committed snapshot of the ledger before touching any
        # slow input.  WAL keeps it readable for the whole export, so an
        # apply committing mid-export can never splice a second ledger
        # version into this manifest.
        conn.execute("SELECT 1 FROM readings LIMIT 1").fetchone()
        conn.execute("DROP TABLE IF EXISTS temp.active")
        conn.execute("CREATE TEMP TABLE active ("
                     "job TEXT PRIMARY KEY, n INTEGER NOT NULL)")
        received, kept = load_assignments(conn, assign_path, wanted)
        writer = OutputWriter(output_path)
        emitted = 0
        for device in sorted(wanted):
            emitted += settle_device(conn, device, t_from, t_to, writer)
        writer.commit()
        conn.commit()
        return received, kept, emitted
    except BaseException:
        conn.rollback()
        if writer is not None:
            writer.abort()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Offline synchronisation: identity, packs, replicas
# ---------------------------------------------------------------------------

def sync_init(db_path):
    """Enable the centre role on a database and fix its sync identity.

    An already populated ledger snapshots every current highest version
    (tombstones included) as position 1; an empty ledger starts at 0.
    The identity, position and change log are written in one
    transaction and can never be replaced afterwards.
    """
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        if replica_state(conn) is not None:
            raise SyncError("this database is a sync replica and cannot "
                            "be turned into a centre")
        existing = center_state(conn)
        if existing is not None:
            raise SyncError(
                f"sync is already enabled on this centre with identity "
                f"{existing[0]!r}; the identity cannot be replaced")
        identity = uuid.uuid4().hex
        populated = conn.execute(
            "SELECT 1 FROM readings LIMIT 1").fetchone() is not None
        if populated:
            position = 1
            conn.execute(
                "INSERT INTO sync_log (device, reading_id, last_version) "
                "SELECT device, reading_id, 1 FROM readings")
        else:
            position = 0
        conn.execute(
            "INSERT INTO sync_center (id, identity, position) "
            "VALUES (1, ?, ?)", (identity, position))
        conn.commit()
        return identity, position
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


class _DigestWriter:
    """Binary file wrapper that hashes every byte written through it."""

    def __init__(self, fh):
        self.fh = fh
        self.hash = hashlib.sha256()

    def write(self, data):
        self.fh.write(data)
        self.hash.update(data)

    def write_raw(self, data):
        """Write without hashing (used for the trailing digest itself)."""
        self.fh.write(data)


def _pack_frame(writer, payload):
    writer.write(len(payload).to_bytes(4, "little"))
    writer.write(payload)


def _pack_row_bytes(row):
    device, reading_id, revision, op, epoch, time, counter = row
    obj = {"device": device, "reading_id": reading_id,
           "revision": revision, "op": op}
    if op == "put":
        obj.update(epoch=epoch, time=time, counter=counter)
    return json.dumps(obj, ensure_ascii=True,
                      separators=(",", ":")).encode("utf-8")


def build_pack(conn, base, output_path):
    """Stream one increment pack onto one committed snapshot of `conn`.

    Returns {"status": "no-update", ...} when base == the current
    position (the output path is not touched), otherwise a "packed"
    descriptor.  The caller owns the transaction; this function only
    reads and must run inside an open snapshot.
    """
    center = center_state(conn)
    if center is None:
        if replica_state(conn) is not None:
            raise SyncError("a sync replica cannot build centre packs")
        raise SyncError("sync is not enabled on this database; run "
                        "sync-init first")
    identity, position = center
    if not _is_int(base) or base < 0:
        raise SyncError("base must be a non-negative integer")
    if base > position:
        raise SyncError(
            f"base position {base} is beyond the current position "
            f"{position}")
    if base == position:
        return {"status": "no-update", "source": identity,
                "position": position}
    end = position
    if base == 0:
        count_sql = "SELECT COUNT(*) FROM readings"
        rows_sql = ("SELECT device, reading_id, revision, op, epoch, time, "
                    "counter FROM readings ORDER BY device, reading_id")
        params = ()
    else:
        # Only identities whose latest logged change is newer than the
        # base participate; untouched history is never scanned.
        count_sql = ("SELECT COUNT(*) FROM sync_log WHERE last_version > ?")
        rows_sql = (
            "SELECT r.device, r.reading_id, r.revision, r.op, r.epoch, "
            "r.time, r.counter FROM sync_log AS s JOIN readings AS r "
            "ON r.device = s.device AND r.reading_id = s.reading_id "
            "WHERE s.last_version > ? ORDER BY r.device, r.reading_id")
        params = (base,)
    row_count = conn.execute(count_sql, params).fetchone()[0]
    header = json.dumps(
        {"format": PACK_FORMAT, "source": identity,
         "base": base, "end": end},
        ensure_ascii=True, separators=(",", ":")).encode("utf-8")

    def write_pack(fh):
        """Write the whole pack to binary file object `fh`; return digest."""
        writer = _DigestWriter(fh)
        writer.write(PACK_MAGIC)
        _pack_frame(writer, header)
        cursor = conn.execute(rows_sql, params)
        while True:
            row = cursor.fetchone()
            if row is None:
                break
            _pack_frame(writer, _pack_row_bytes(row))
        writer.write((0).to_bytes(4, "little"))  # end-of-rows marker
        fh.flush()
        digest = writer.hash.digest()
        writer.write_raw(digest)                 # not covered by itself
        return digest

    if output_path == "-":
        digest = write_pack(sys.stdout.buffer)
        sys.stdout.buffer.flush()
    else:
        directory = os.path.dirname(os.path.abspath(output_path))
        base_name = os.path.basename(output_path)
        try:
            fd, tmp_path = tempfile.mkstemp(
                prefix=f".{base_name}.", suffix=".tmp", dir=directory)
        except OSError as exc:
            raise LedgerIOError(
                f"cannot create temporary pack next to {output_path!r}: "
                f"{exc.strerror or exc}") from exc
        try:
            with os.fdopen(fd, "wb") as fh:
                digest = write_pack(fh)
                os.fsync(fh.fileno())
            os.replace(tmp_path, output_path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    return {"status": "packed", "source": identity, "base": base,
            "end": end, "rows": row_count, "digest": digest.hex(),
            "path": output_path}


def run_sync_pack(db_path, base, output_path):
    conn = connect(db_path)
    try:
        # A deferred transaction on WAL pins the snapshot at the first
        # read; a concurrent apply may commit afterwards and simply
        # belongs to the next pack, while this pack's end and rows stay
        # two views of the same committed version.
        conn.execute("BEGIN")
        conn.execute("SELECT 1 FROM readings LIMIT 1").fetchone()
        result = build_pack(conn, base, output_path)
        conn.commit()
        return result
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


class _HashedReader:
    """Binary reader that hashes every byte it consumes."""

    def __init__(self, fh):
        self.fh = fh
        self.hash = hashlib.sha256()

    def read(self, size):
        data = self.fh.read(size)
        self.hash.update(data)
        return data

    def read_raw(self, size):
        """Read without hashing (used for the trailing digest itself)."""
        return self.fh.read(size)

    def read_exact(self, size, what):
        data = self.read(size)
        if len(data) != size:
            raise SyncError(f"truncated pack while reading {what}")
        return data


def _validate_header(obj):
    if not isinstance(obj, dict):
        raise SyncError("pack header must be a JSON object")
    if obj.get("format") != PACK_FORMAT:
        raise SyncError(f"unsupported pack format {obj.get('format')!r}")
    source = obj.get("source")
    if not isinstance(source, str) or not source:
        raise SyncError("pack header 'source' must be a non-empty string")
    base = obj.get("base")
    end = obj.get("end")
    if not _is_int(base) or base < 0:
        raise SyncError("pack header 'base' must be a non-negative integer")
    if not _is_int(end) or end < base:
        raise SyncError("pack header 'end' must be an integer >= base")
    return {"format": PACK_FORMAT, "source": source, "base": base,
            "end": end}


def verify_pack(pack_path, sink=None):
    """Fully verify a pack file, streaming each row to `sink`.

    `sink` receives the tuple returned by validate_message.  Detects
    truncation, altered content (digest mismatch) and extra trailing
    bytes.  Returns (header, row_count, digest_hex).  Nothing in the
    pack is trusted before the digest has checked out.
    """
    try:
        fh = open(pack_path, "rb")
    except OSError as exc:
        raise SyncError(
            f"cannot read pack {pack_path!r}: {exc.strerror or exc}") \
            from exc
    row_count = 0
    with fh:
        reader = _HashedReader(fh)
        if reader.read_exact(len(PACK_MAGIC), "magic") != PACK_MAGIC:
            raise SyncError("not a shift-ledger pack (bad magic)")
        length = int.from_bytes(
            reader.read_exact(4, "header length"), "little")
        if not 0 < length <= PACK_MAX_FRAME:
            raise SyncError(f"implausible header frame length {length}")
        try:
            header = _validate_header(json.loads(
                reader.read_exact(length, "header").decode("utf-8")))
        except (ValueError, UnicodeDecodeError) as exc:
            raise SyncError(f"invalid pack header: {exc}") from exc
        while True:
            length = int.from_bytes(
                reader.read_exact(4, "frame length"), "little")
            if length == 0:
                break
            if length > PACK_MAX_FRAME:
                raise SyncError(f"implausible frame length {length}")
            raw = reader.read_exact(length, "row frame")
            try:
                obj = json.loads(raw.decode("utf-8"))
            except (ValueError, UnicodeDecodeError) as exc:
                raise SyncError(f"invalid row frame: {exc}") from exc
            try:
                message = validate_message(obj, f"pack row {row_count + 1}")
            except BatchError as exc:
                raise SyncError(str(exc)) from exc
            if sink is not None:
                sink(message)
            row_count += 1
        # The digest covers everything before it; read it without
        # feeding it back into the hash.
        digest = reader.read_raw(PACK_DIGEST_SIZE)
        if len(digest) != PACK_DIGEST_SIZE:
            raise SyncError("truncated pack while reading digest")
        if reader.fh.read(1):
            raise SyncError("pack has extra trailing bytes after the "
                            "digest")
    if not hmac.compare_digest(digest, reader.hash.digest()):
        raise SyncError("pack digest mismatch: content altered or corrupt")
    if header["end"] == header["base"] and row_count:
        raise SyncError("pack ending at its base position must carry no "
                        "rows")
    return header, row_count, reader.hash.hexdigest()


def receive_pack(db_path, pack_path):
    """Apply a verified pack to a replica database in one transaction."""
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        if center_state(conn) is not None:
            raise SyncError("a centre database cannot receive replica "
                            "packs")
        conn.execute("DROP TABLE IF EXISTS temp.batch")
        conn.execute(
            "CREATE TEMP TABLE batch ("
            "device TEXT NOT NULL, reading_id TEXT NOT NULL, "
            "revision INTEGER NOT NULL, op TEXT NOT NULL, "
            "epoch TEXT, time INTEGER, counter INTEGER, "
            "content TEXT NOT NULL)")
        insert = ("INSERT INTO batch (device, reading_id, revision, op, "
                  "epoch, time, counter, content) "
                  "VALUES (?,?,?,?,?,?,?,?)")
        # The whole file — digest included — is verified before any
        # persistent table is touched; rows only land in a temp table.
        header, row_count, digest_hex = verify_pack(
            pack_path, sink=lambda msg: conn.execute(insert, msg))
        try:
            duplicate = conn.execute(
                "SELECT device, reading_id FROM temp.batch "
                "GROUP BY device, reading_id HAVING COUNT(*) > 1 "
                "LIMIT 1").fetchone()
        except sqlite3.Error as exc:
            raise SyncError(f"invalid pack contents: {exc}") from exc
        if duplicate is not None:
            raise SyncError(
                f"pack carries more than one row for reading "
                f"{duplicate[1]!r} of device {duplicate[0]!r}; a pack "
                f"holds one end-state row per identity")
        replica = replica_state(conn)
        if replica is None:
            if header["base"] != 0:
                raise SyncError(
                    f"the first pack must have base 0 for an empty "
                    f"unbound database; got base {header['base']}")
            for table in ("readings", "confirmed", "uncertain_edge"):
                if conn.execute(
                        f"SELECT COUNT(*) FROM {table}").fetchone()[0]:
                    raise SyncError(
                        "the first pack can only enter an empty database")
        else:
            source, position, last_digest = replica
            if header["source"] != source:
                raise SyncError(
                    f"pack source {header['source']!r} does not match the "
                    f"bound source {source!r}")
            if digest_hex == last_digest:
                # Byte-identical re-delivery of the last accepted file:
                # a successful no-op.  Its base is the old position by
                # definition, so recognise it before the base check.
                # A different file with the same end position has a
                # different digest and is refused below.
                conn.rollback()
                return {"status": "replay", "source": source,
                        "position": position, "rows": 0}
            if header["base"] != position:
                raise SyncError(
                    f"pack base {header['base']} does not equal the "
                    f"replica position {position}: missing, reordered or "
                    f"foreign packs are not merged")
            if header["end"] < position:
                raise SyncError(
                    f"pack end {header['end']} is behind the replica "
                    f"position {position}")
        try:
            applied = apply_winners(conn)
            check_time_conflicts(conn)
        except (BatchError, sqlite3.Error) as exc:
            raise SyncError(f"pack cannot be applied: {exc}") from exc
        conn.execute(
            "INSERT INTO sync_replica (id, source, position, last_digest) "
            "VALUES (1, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET "
            "source = excluded.source, position = excluded.position, "
            "last_digest = excluded.last_digest",
            (header["source"], header["end"], digest_hex))
        conn.commit()
        return {"status": "received", "source": header["source"],
                "position": header["end"], "rows": applied}
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------

def build_parser():
    parser = argparse.ArgumentParser(
        prog="shift_ledger.py",
        description="Versioned SQLite ledger of cumulative counter "
                    "readings with per-shift JSONL accounting export.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_apply = sub.add_parser(
        "apply", help="apply one inspect_rebuild.py JSONL file as one "
                      "atomic batch")
    p_apply.add_argument("db", help="SQLite ledger path")
    p_apply.add_argument("input", help="JSONL file to apply, or '-' for "
                                       "stdin")

    p_query = sub.add_parser(
        "query", help="emit one JSONL line per shift in [START, END)")
    p_query.add_argument("db", help="SQLite ledger path")
    p_query.add_argument("device", help="device to report")
    p_query.add_argument("start", type=int,
                         help=f"first shift start (multiple of {SHIFT})")
    p_query.add_argument("end", type=int,
                         help=f"exclusive end shift start "
                              f"(multiple of {SHIFT})")
    p_query.add_argument("-o", "--output", default="-",
                         help="JSONL destination; '-' means stdout "
                              "(default)")

    p_settle = sub.add_parser(
        "settle", help="export a settlement manifest reconciling metering "
                       "intervals with the assignment timeline")
    p_settle.add_argument("db", help="SQLite ledger path")
    p_settle.add_argument("--from", dest="from_", type=int, required=True,
                          metavar="FROM",
                          help="first second of the settlement range "
                               "(inclusive)")
    p_settle.add_argument("--to", dest="to_", type=int, required=True,
                          metavar="TO",
                          help="end of the settlement range (exclusive); "
                               "may be 2^53")
    p_settle.add_argument("--assign", required=True, metavar="ASSIGNMENTS",
                          help="assignment JSONL file, or '-' for stdin")
    p_settle.add_argument("-o", "--output", default="-",
                          help="manifest destination; '-' means stdout "
                               "(default)")
    p_settle.add_argument("devices", nargs="+", metavar="DEVICE",
                          help="device(s) to settle; duplicates are "
                               "settled once")

    p_init = sub.add_parser(
        "sync-init", help="enable the centre role on a database and "
                          "persist its fixed sync identity")
    p_init.add_argument("db", help="SQLite ledger path")

    p_pack = sub.add_parser(
        "sync-pack", help="publish an increment pack from a centre "
                          "database for one base position")
    p_pack.add_argument("db", help="centre SQLite ledger path")
    p_pack.add_argument("--base", type=int, required=True, metavar="N",
                        help="base position (non-negative); 0 means a "
                             "full pack from blank")
    p_pack.add_argument("-o", "--output", default="-",
                        help="pack destination; '-' means stdout "
                             "(default)")

    p_recv = sub.add_parser(
        "sync-receive", help="apply a verified pack to a branch replica")
    p_recv.add_argument("db", help="replica SQLite ledger path")
    p_recv.add_argument("pack", help="pack file received from the centre")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        if args.command == "apply":
            received, applied = apply_batch(args.db, args.input)
            print(f"shift_ledger: applied {applied} of {received} "
                  f"message(s)", file=sys.stderr)
        elif args.command == "query":
            run_query(args.db, args.device, args.start, args.end,
                      args.output)
        elif args.command == "sync-init":
            identity, position = sync_init(args.db)
            print(f"shift_ledger: sync enabled; identity={identity} "
                  f"position={position}", file=sys.stderr)
        elif args.command == "sync-pack":
            result = run_sync_pack(args.db, args.base, args.output)
            if result["status"] == "no-update":
                print(f"shift_ledger: no update: base {args.base} is the "
                      f"current position of source {result['source']}; "
                      f"target file not touched", file=sys.stderr)
            else:
                print(f"shift_ledger: pack {result['base']} -> "
                      f"{result['end']} ({result['rows']} identity "
                      f"row(s), source {result['source']})", file=sys.stderr)
        elif args.command == "sync-receive":
            result = receive_pack(args.db, args.pack)
            if result["status"] == "replay":
                print(f"shift_ledger: identical pack replayed; replica "
                      f"still at position {result['position']} "
                      f"(source {result['source']})", file=sys.stderr)
            else:
                print(f"shift_ledger: received {result['rows']} "
                      f"identity row(s); replica now at position "
                      f"{result['position']} (source "
                      f"{result['source']})", file=sys.stderr)
        else:
            received, kept, emitted = run_settle(
                args.db, args.devices, args.from_, args.to_,
                args.assign, args.output)
            print(f"shift_ledger: exported {emitted} row(s) from {kept} "
                  f"of {received} assignment(s)", file=sys.stderr)
    except (BatchError, QueryError, AssignmentError, SyncError) as exc:
        print(f"shift_ledger: invalid input: {exc}", file=sys.stderr)
        return 2
    except LedgerIOError as exc:
        print(f"shift_ledger: I/O failure: {exc}", file=sys.stderr)
        return 1
    except sqlite3.Error as exc:
        print(f"shift_ledger: database failure: {exc}", file=sys.stderr)
        return 1
    except BrokenPipeError:
        return 1
    except Exception:
        print("shift_ledger: unexpected failure:", file=sys.stderr)
        traceback.print_exc()
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
