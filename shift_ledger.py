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

Usage:
    python3 -I shift_ledger.py apply DB JSONL
    python3 -I shift_ledger.py query DB DEVICE START END [-o OUTPUT]

Exit status:
    0  success
    2  invalid input (bad JSONL/message, version or time conflict, bad
       query range)
    1  I/O or unexpected failure
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import tempfile
import traceback

SHIFT = 28800
MAX_REVISION = (1 << 63) - 1
MAX_TIME = (1 << 53) - 1
MAX_COUNTER = 10 ** 9
MAX_BOUND = (1 << 63) - 1


class BatchError(Exception):
    """The input file or its messages are invalid; nothing was applied."""


class QueryError(Exception):
    """The query arguments are invalid."""


class LedgerIOError(Exception):
    """The database or an output file could not be opened or written."""


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
        received = load_batch(conn, input_path)
        applied = apply_winners(conn)
        check_time_conflicts(conn)
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
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        if args.command == "apply":
            received, applied = apply_batch(args.db, args.input)
            print(f"shift_ledger: applied {applied} of {received} "
                  f"message(s)", file=sys.stderr)
        else:
            run_query(args.db, args.device, args.start, args.end,
                      args.output)
    except (BatchError, QueryError) as exc:
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
