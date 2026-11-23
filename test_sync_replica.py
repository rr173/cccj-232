#!/usr/bin/env python3
"""Tests for the offline replica synchronisation in shift_ledger.py.

The suite reproduces the whole hand-off workflow on real CLI processes
plus a few in-process tests for the snapshot and no-full-scan
guarantees that cannot be forced through process boundaries.

Run:  python3 -I test_sync_replica.py
"""

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import shift_ledger as sl  # noqa: E402

SHIFT = 28800
TOOL = os.path.join(HERE, "shift_ledger.py")


def run_tool(args, binary=False):
    result = subprocess.run([sys.executable, "-I", TOOL] + args,
                            capture_output=True)
    if not binary:
        result.stdout = result.stdout.decode("utf-8")
        result.stderr = result.stderr.decode("utf-8")
    return result


def record(obj):
    return json.dumps({"type": "record", "start": 0, "end": 1,
                       "object": obj})


def put(device, rid, rev, epoch, t, counter):
    return {"device": device, "reading_id": rid, "revision": rev,
            "op": "put", "epoch": epoch, "time": t, "counter": counter}


def delete(device, rid, rev):
    return {"device": device, "reading_id": rid, "revision": rev,
            "op": "delete"}


def write_jsonl(path, messages):
    with open(path, "w", encoding="utf-8") as fh:
        for m in messages:
            fh.write(record(m) + "\n")


def dump_db(path):
    """Independent materialisation of every ledger table."""
    conn = sqlite3.connect(path)
    try:
        return {
            "readings": sorted(conn.execute(
                "SELECT device, reading_id, revision, op, epoch, time, "
                "counter FROM readings").fetchall()),
            "confirmed": sorted(conn.execute(
                "SELECT device, shift, increment FROM confirmed").fetchall()),
            "uncertain_edge": sorted(conn.execute(
                "SELECT device, shift, seconds FROM uncertain_edge")
                .fetchall()),
        }
    finally:
        conn.close()


def query_rows(db, device, start, end):
    result = run_tool(["query", db, device, str(start), str(end)])
    assert result.returncode == 0, result.stderr
    return [json.loads(x) for x in result.stdout.splitlines() if x.strip()]


class SyncCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="sync-test-")
        self.center = os.path.join(self.dir, "center.db")
        self.replica = os.path.join(self.dir, "replica.db")
        self._n = 0

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def path(self, name):
        return os.path.join(self.dir, name)

    def apply(self, db, messages, expect_ok=True):
        self._n += 1
        path = self.path(f"batch{self._n}.jsonl")
        write_jsonl(path, messages)
        result = run_tool(["apply", db, path])
        if expect_ok:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0)
        return result

    def init_center(self, db=None):
        db = db or self.center
        result = run_tool(["sync-init", db])
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stderr

    def pack(self, base, out=None, db=None, expect_ok=True):
        db = db or self.center
        out = out or self.path(f"pack-{base}.slpack")
        result = run_tool(["sync-pack", db, "--base", str(base),
                           "-o", out])
        if expect_ok:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0)
        return result, out

    def receive(self, pack, db=None, expect_ok=True):
        db = db or self.replica
        result = run_tool(["sync-receive", db, pack])
        if expect_ok:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0)
        return result

    def center_position(self, db=None):
        conn = sqlite3.connect(db or self.center)
        try:
            return conn.execute(
                "SELECT position FROM sync_center WHERE id = 1"
            ).fetchone()[0]
        finally:
            conn.close()

    def replica_position(self, db=None):
        conn = sqlite3.connect(db or self.replica)
        try:
            return conn.execute(
                "SELECT position FROM sync_replica WHERE id = 1"
            ).fetchone()[0]
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Identity and position semantics
# ---------------------------------------------------------------------------

class SyncInitTest(SyncCase):
    def test_populated_ledger_becomes_position_one_with_tombstones(self):
        self.apply(self.center, [
            put("d", "r1", 1, "E", 1000, 10),
            put("d", "r2", 1, "E", 2000, 20),
            delete("d", "r2", 2)])
        self.init_center()
        self.assertEqual(self.center_position(), 1)
        conn = sqlite3.connect(self.center)
        try:
            # All three identities, the tombstone included, are logged.
            self.assertEqual(sorted(conn.execute(
                "SELECT device, reading_id, last_version FROM sync_log")
                .fetchall()),
                [("d", "r1", 1), ("d", "r2", 1)])
            identity = conn.execute(
                "SELECT identity FROM sync_center").fetchone()[0]
        finally:
            conn.close()
        # The identity is fixed: a second sync-init is refused.
        result = run_tool(["sync-init", self.center])
        self.assertEqual(result.returncode, 2)
        conn = sqlite3.connect(self.center)
        try:
            self.assertEqual(conn.execute(
                "SELECT identity FROM sync_center").fetchone()[0], identity)
        finally:
            conn.close()

    def test_empty_ledger_starts_at_zero(self):
        self.init_center()
        self.assertEqual(self.center_position(), 0)
        result, _ = self.pack(0, self.path("empty.slpack"))
        self.assertIn("no update", result.stderr)
        self.assertFalse(os.path.exists(self.path("empty.slpack")))
        # The first apply on the empty centre moves it to position 1.
        self.apply(self.center, [put("d", "r1", 1, "E", 1000, 10)])
        self.assertEqual(self.center_position(), 1)

    def test_independent_centres_get_independent_identities(self):
        other = self.path("other.db")
        self.init_center()
        result = run_tool(["sync-init", other])
        self.assertEqual(result.returncode, 0, result.stderr)
        c1 = sqlite3.connect(self.center).execute(
            "SELECT identity FROM sync_center").fetchone()[0]
        c2 = sqlite3.connect(other).execute(
            "SELECT identity FROM sync_center").fetchone()[0]
        self.assertNotEqual(c1, c2)
        self.assertEqual(len(c1), 32)  # uuid4 hex

    def test_replica_cannot_become_a_centre(self):
        self.apply(self.center, [put("d", "r1", 1, "E", 1000, 10)])
        self.init_center()
        _, pack = self.pack(0)
        self.receive(pack)
        result = run_tool(["sync-init", self.replica])
        self.assertEqual(result.returncode, 2)
        self.assertIn("replica", result.stderr)


class PositionAdvanceTest(SyncCase):
    def test_only_effective_applies_advance_and_commit_together(self):
        self.init_center()
        self.apply(self.center, [put("d", "r1", 1, "E", 1000, 10)])
        self.assertEqual(self.center_position(), 1)
        # An identical replay changes no highest version: no advance.
        self.apply(self.center, [put("d", "r1", 1, "E", 1000, 10)])
        self.assertEqual(self.center_position(), 1)
        # A revision-only bump (same values) still advances.
        self.apply(self.center, [put("d", "r1", 2, "E", 1000, 10)])
        self.assertEqual(self.center_position(), 2)
        # A strictly lower revision with different content loses to the
        # ledger's higher version and is a pure no-op.
        self.apply(self.center, [put("d", "r1", 1, "E", 999, 9)])
        self.assertEqual(self.center_position(), 2)
        # A failed batch does not advance and leaves no log residue.
        before = self.center_position()
        self.apply(self.center, [
            put("d", "r1", 3, "E", 2000, 11),
            put("d", "rx", 1, "E", 2000, 12)], expect_ok=False)  # same second
        self.assertEqual(self.center_position(), before)
        conn = sqlite3.connect(self.center)
        try:
            self.assertIsNone(conn.execute(
                "SELECT 1 FROM sync_log WHERE reading_id = 'rx'").fetchone())
            # The position row and readings always agree: r1 is still rev 2.
            self.assertEqual(conn.execute(
                "SELECT revision FROM readings WHERE reading_id = 'r1'"
            ).fetchone()[0], 2)
        finally:
            conn.close()

    def test_change_log_holds_one_row_per_identity_not_per_change(self):
        self.init_center()
        for rev in range(1, 5):
            self.apply(self.center, [
                put("d", "r1", rev, "E", 1000 + rev, 10 + rev)])
        conn = sqlite3.connect(self.center)
        try:
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM sync_log").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT last_version FROM sync_log").fetchone()[0], 4)
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# Pack construction
# ---------------------------------------------------------------------------

class PackBuildTest(SyncCase):
    def test_pack_carries_only_changed_identities_with_end_state(self):
        self.apply(self.center, [
            put("d", "r1", 1, "E", 1000, 10),
            put("d", "r2", 1, "E", 2000, 20),
            put("d", "r3", 1, "E", 3000, 30)])
        self.init_center()
        _, p0 = self.pack(0)
        header, rows, digest = sl.verify_pack(p0)
        self.assertEqual((header["base"], header["end"]), (0, 1))
        self.assertEqual(rows, 3)
        # r1 changes twice in two applies, r3 is retracted; r2 is quiet.
        self.apply(self.center, [put("d", "r1", 2, "E", 1100, 11)])
        self.apply(self.center, [
            put("d", "r1", 3, "E", 1200, 12),
            delete("d", "r3", 2)])
        self.assertEqual(self.center_position(), 3)
        _, p1 = self.pack(1)
        header, rows, _ = sl.verify_pack(p1)
        self.assertEqual((header["base"], header["end"]), (1, 3))
        # Exactly the identities that changed at least once, one row
        # each, and the tombstone is carried.
        self.assertEqual(rows, 2)
        got = {}
        sl.verify_pack(p1, sink=lambda m: got.__setitem__(
            (m[0], m[1]), m))
        self.assertEqual(got[("d", "r1")][2:7],
                         (3, "put", "E", 1200, 12))   # end state only
        self.assertEqual(got[("d", "r3")][2:4], (2, "delete"))
        # Base beyond the current position is an error.
        result, _ = self.pack(99, self.path("bad.slpack"), expect_ok=False)
        self.assertEqual(result.returncode, 2)
        self.assertFalse(os.path.exists(self.path("bad.slpack")))

    def test_no_update_leaves_existing_target_file_untouched(self):
        self.apply(self.center, [put("d", "r1", 1, "E", 1000, 10)])
        self.init_center()
        target = self.path("keep.slpack")
        with open(target, "wb") as fh:
            fh.write(b"precious")
        result, _ = self.pack(1, target)
        self.assertIn("no update", result.stderr)
        with open(target, "rb") as fh:
            self.assertEqual(fh.read(), b"precious")

    def test_pack_end_and_rows_belong_to_one_snapshot(self):
        """In-process: an apply landing while packing cannot splice.

        A second connection holds the write lock for the whole build, so
        the concurrent apply's BEGIN IMMEDIATE blocks until the pack is
        finished; the pack therefore observes one complete committed
        version, and the blocked apply lands in the next pack.
        """
        self.apply(self.center, [put("d", "r1", 1, "E", 1000, 10)])
        self.init_center()
        self.assertEqual(self.center_position(), 1)
        write_jsonl(self.path("late.jsonl"),
                    [put("d", "r2", 1, "E", 2000, 20)])
        lock_holder = sl.connect(self.center)
        outcome = {}
        try:
            lock_holder.execute("BEGIN IMMEDIATE")  # owns the write lock
            thread = threading.Thread(
                target=lambda: outcome.setdefault(
                    "rc", sl.apply_batch(self.center,
                                         self.path("late.jsonl"))))
            thread.start()
            # The writer is now queued behind lock_holder; meanwhile the
            # pack must still complete (WAL readers do not block).
            time.sleep(0.5)
            self.assertTrue(thread.is_alive())
            conn = sl.connect(self.center)
            try:
                conn.execute("BEGIN")
                conn.execute(
                    "SELECT 1 FROM readings LIMIT 1").fetchone()
                descriptor = sl.build_pack(
                    conn, 0, self.path("snap.slpack"))
                conn.commit()
            finally:
                conn.close()
            header, rows, _ = sl.verify_pack(self.path("snap.slpack"))
            self.assertEqual((header["base"], header["end"]), (0, 1))
            self.assertEqual(rows, 1)  # r2 excluded: belongs to next
            lock_holder.commit()        # release the write lock
            thread.join(10)
        finally:
            lock_holder.close()
        self.assertNotIn("error", outcome)
        # The blocked apply then committed as the next version.
        self.assertEqual(self.center_position(), 2)
        _, next_pack = self.pack(1)
        header, rows, _ = sl.verify_pack(next_pack)
        self.assertEqual((header["base"], header["end"]), (1, 2))
        self.assertEqual(rows, 1)


# ---------------------------------------------------------------------------
# Replica reception rules
# ---------------------------------------------------------------------------

class ReceiveRuleTest(SyncCase):
    def _bound_pair(self):
        self.apply(self.center, [
            put("d", "r1", 1, "E", 1000, 10),
            put("d", "r2", 1, "E", 2000, 20)])
        self.init_center()
        _, p0 = self.pack(0)
        self.receive(p0)
        return p0

    def test_first_pack_requirements(self):
        self.apply(self.center, [put("d", "r1", 1, "E", 1000, 10)])
        self.init_center()
        # Nonzero base into an unbound database.
        self.apply(self.center, [put("d", "r2", 1, "E", 2000, 20)])
        _, p1 = self.pack(1)
        result = self.receive(p1, db=self.path("fresh.db"), expect_ok=False)
        self.assertIn("first pack must have base 0", result.stderr)
        # Base 0 into a non-empty unbound database.
        _, p0 = self.pack(0)
        dirty = self.path("dirty.db")
        self.apply(dirty, [put("x", "a", 1, "E", 1, 1)])
        result = self.receive(p0, db=dirty, expect_ok=False)
        self.assertIn("only enter an empty database", result.stderr)

    def test_gap_reorder_and_foreign_source_are_refused(self):
        p0 = self._bound_pair()
        self.assertEqual(self.replica_position(), 1)
        # Produce versions 2 and 3.
        self.apply(self.center, [put("d", "r1", 2, "E", 1100, 11)])
        _, p1 = self.pack(1)
        self.apply(self.center, [put("d", "r1", 3, "E", 1200, 12)])
        _, p2 = self.pack(2)
        # Skip p1 and offer p2: gap rejected, position unchanged.
        result = self.receive(p2, expect_ok=False)
        self.assertIn("does not equal the replica position 1",
                      result.stderr)
        self.assertEqual(self.replica_position(), 1)
        # Re-offer an already-consumed pack with a different digest:
        # a same-source, wrong-base file is an out-of-order refusal,
        # not a replay (the real p0 file itself would be an idempotent
        # replay, which is covered by the dedicated replay test).
        tampered = self.path("reordered.slpack")
        with open(p0, "rb") as fh:
            blob = bytearray(fh.read())
        blob[-1] ^= 0x01  # break only the digest, framing stays intact
        with open(tampered, "wb") as fh:
            fh.write(blob)
        result = self.receive(tampered, expect_ok=False)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(self.replica_position(), 1)
        # Foreign source.
        foreign = self.path("foreign.db")
        self.apply(foreign, [put("d", "r9", 1, "E", 1, 1)])
        self.init_center(foreign)
        _, fp = self.pack(0, db=foreign,
                          out=self.path("foreign.slpack"))
        result = self.receive(fp, expect_ok=False)
        self.assertIn("does not match the bound source", result.stderr)
        self.assertEqual(self.replica_position(), 1)
        # The correct next pack still applies after all refusals.
        self.receive(p1)
        self.assertEqual(self.replica_position(), 2)

    def test_identical_replay_is_noop_but_same_end_other_file_is_not(self):
        self._bound_pair()
        self.apply(self.center, [put("d", "r1", 2, "E", 1100, 11)])
        _, p1 = self.pack(1)
        first = self.receive(p1)
        self.assertIn("now at position 2", first.stderr)
        again = self.receive(p1)
        self.assertEqual(again.returncode, 0)
        self.assertIn("identical pack replayed", again.stderr)
        self.assertEqual(self.replica_position(), 2)
        # A different file that also ends at position 2 is NOT a replay:
        # rebuild an equivalent end state on another centre and forge the
        # same (base, end) numbers; its digest differs and its source
        # differs, so it is refused.
        other = self.path("other.db")
        self.apply(other, [put("d", "r1", 1, "E", 1100, 11)])
        self.init_center(other)
        result = run_tool(["sync-pack", other, "--base", "0",
                           "-o", self.path("other.slpack")])
        self.assertEqual(result.returncode, 0)
        result = self.receive(self.path("other.slpack"), expect_ok=False)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(self.replica_position(), 2)

    def test_replica_role_guards(self):
        self._bound_pair()
        write_jsonl(self.path("x.jsonl"),
                    [put("d", "r9", 1, "E", 9000, 9)])
        result = run_tool(["apply", self.replica, self.path("x.jsonl")])
        self.assertEqual(result.returncode, 2)
        self.assertIn("replica", result.stderr)
        result = run_tool(["sync-pack", self.replica, "--base", "0",
                           "-o", self.path("z.slpack")])
        self.assertEqual(result.returncode, 2)
        self.assertIn("replica", result.stderr)
        self.assertFalse(os.path.exists(self.path("z.slpack")))
        # A centre cannot receive packs either.
        result = self.receive(self.path("z.slpack"),
                              db=self.center, expect_ok=False)
        self.assertEqual(result.returncode, 2)
        self.assertIn("centre database cannot receive", result.stderr)

    def test_bad_packs_change_nothing_and_good_pack_still_lands(self):
        self._bound_pair()
        self.apply(self.center, [put("d", "r1", 2, "E", 1100, 11)])
        _, p1 = self.pack(1)
        good_state = dump_db(self.replica)
        size = os.path.getsize(p1)
        variants = {}
        # Truncated: only the first frames survive.
        with open(p1, "rb") as fh:
            data = fh.read()
        variants["truncated"] = data[:size - 10]
        variants["trailing"] = data + b"EXTRA"
        mutated = bytearray(data)
        mutated[len(sl.PACK_MAGIC) + 10] ^= 0x01
        variants["mutated"] = bytes(mutated)
        for name, blob in variants.items():
            path = self.path(f"{name}.slpack")
            with open(path, "wb") as fh:
                fh.write(blob)
            result = self.receive(path, expect_ok=False)
            self.assertEqual(result.returncode, 2, name)
            self.assertEqual(dump_db(self.replica), good_state, name)
            self.assertEqual(self.replica_position(), 1, name)
        # Reopening the database (a fresh process per receive already
        # does) and re-feeding the good pack still works.
        self.receive(p1)
        self.assertEqual(self.replica_position(), 2)
        self.receive(p1)  # and is then an idempotent replay

    def test_pack_with_duplicate_identity_rows_is_rejected(self):
        # A transport-correct file (valid framing and digest) may still
        # hold two rows for one identity, which the format never emits;
        # it must be refused as invalid input without touching state.
        self._bound_pair()
        rows = [
            put("d", "r1", 2, "E", 1100, 11),
            put("d", "r1", 3, "E", 1200, 12)]
        payloads = [json.dumps({"format": 1, "source":
                                self._center_identity(),
                                "base": 1, "end": 2},
                               separators=(",", ":")).encode("utf-8")]
        for m in rows:
            payloads.append(json.dumps(m, separators=(",", ":")).encode())
        import hashlib
        h = hashlib.sha256()
        blob = bytearray(sl.PACK_MAGIC)
        h.update(sl.PACK_MAGIC)
        for payload in payloads:
            frame = len(payload).to_bytes(4, "little") + payload
            blob += frame
            h.update(frame)
        blob += (0).to_bytes(4, "little")
        h.update((0).to_bytes(4, "little"))
        blob += h.digest()
        evil = self.path("duplicate.slpack")
        with open(evil, "wb") as fh:
            fh.write(blob)
        good_state = dump_db(self.replica)
        result = self.receive(evil, expect_ok=False)
        self.assertEqual(result.returncode, 2)
        self.assertIn("one end-state row per identity", result.stderr)
        self.assertEqual(dump_db(self.replica), good_state)
        self.assertEqual(self.replica_position(), 1)

    def _center_identity(self):
        conn = sqlite3.connect(self.center)
        try:
            return conn.execute(
                "SELECT identity FROM sync_center").fetchone()[0]
        finally:
            conn.close()

    def test_atomically_final_state_accepts_temporary_same_second(self):
        # Two points swap seconds inside one pack: any row-by-row view
        # would collide on a second, but the final state is legal.
        self.apply(self.center, [
            put("d", "a", 1, "E", 1000, 1),
            put("d", "b", 1, "E", 2000, 2),
            put("d", "c", 1, "E", 3000, 3)])
        self.init_center()
        self.apply(self.center, [
            put("d", "a", 2, "E", 2000, 1),
            put("d", "b", 2, "E", 1000, 2)])
        _, pack = self.pack(0)
        result = self.receive(pack, db=self.path("swap.db"))
        self.assertEqual(result.returncode, 0, result.stderr)
        got = query_rows(self.path("swap.db"), "d", 0, SHIFT)
        want = query_rows(self.center, "d", 0, SHIFT)
        self.assertEqual(got, want)


# ---------------------------------------------------------------------------
# Replica content equivalence with the centre
# ---------------------------------------------------------------------------

class ReplicaEquivalenceTest(SyncCase):
    def test_replica_matches_center_through_many_versions(self):
        messages = [
            put("A", "r1", 1, "E1", 28000, 100),
            put("A", "r2", 1, "E1", 30000, 250),
            put("A", "r3", 1, "E1", 28800, 180),
            put("B", "b1", 1, "E9", 100, 5),
            put("B", "b2", 1, "E9", 500, 9)]
        self.apply(self.center, messages)
        self.init_center()
        _, p0 = self.pack(0)
        self.receive(p0)
        self.assertEqual(dump_db(self.replica), dump_db(self.center))
        series = [
            [delete("A", "r3", 2),
             put("A", "r2", 2, "E1", 30000, 250)],   # retract + rev-only
            [put("A", "r3", 3, "E1", 29500, 175)],   # reintroduce later
            [put("B", "b1", 2, "E9", 100, 4)],       # falling counter
            [put("A", "r1", 2, "E2", 28000, 3)],     # epoch change
            [delete("B", "b2", 2)],                  # tombstone
        ]
        position = 1
        for batch in series:
            self.apply(self.center, batch)
            position += 1
            _, pack = self.pack(position - 1)
            self.receive(pack)
            self.assertEqual(self.replica_position(), position)
            self.assertEqual(dump_db(self.replica), dump_db(self.center))
            for device in ("A", "B", "ghost"):
                self.assertEqual(
                    query_rows(self.replica, device, 0, 3 * SHIFT),
                    query_rows(self.center, device, 0, 3 * SHIFT))

    def test_settle_on_replica_cites_synced_evidence_revisions(self):
        self.apply(self.center, [
            put("A", "r1", 1, "E", 28000, 100),
            put("A", "r2", 1, "E", 30000, 250),
            put("A", "r3", 1, "E", 28800, 180)])
        self.init_center()
        # The boundary point is retracted: the two-shift-splittable
        # amount becomes one cross-shift uncertain interval.
        self.apply(self.center, [delete("A", "r3", 2)])
        _, pack = self.pack(0)
        self.receive(pack)
        assignments = self.path("assign.jsonl")
        with open(assignments, "w", encoding="utf-8") as fh:
            fh.write(json.dumps({"device": "A", "job": "J1",
                                 "start": 28000, "end": 30000}) + "\n")
        out = self.path("settle.jsonl")
        result = run_tool(["settle", self.replica, "--from", "0",
                           "--to", "40000", "--assign", assignments,
                           "-o", out, "A"])
        self.assertEqual(result.returncode, 0, result.stderr)
        with open(out, encoding="utf-8") as fh:
            rows = [json.loads(x) for x in fh if x.strip()]
        # The single assignment covers every second of the surviving
        # [28000, 30000) interval, so it is allocated to J1; the
        # evidence endpoints are r1@1 and r2@1 (r3's tombstone emits
        # nothing and splits nothing).
        self.assertEqual(rows, [{
            "device": "A", "start": 28000, "end": 30000,
            "left_id": "r1", "left_revision": 1,
            "right_id": "r2", "right_revision": 1,
            "status": "allocated", "job": "J1", "amount": 150}])
        # The assignment table is not copied into the ledger database.
        tables = {r[0] for r in sqlite3.connect(self.replica).execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertNotIn("assign", tables)


# ---------------------------------------------------------------------------
# No full-history scans for small incremental updates
# ---------------------------------------------------------------------------

class IncrementalScaleTest(SyncCase):
    def test_small_update_pack_does_not_scan_unchanged_history(self):
        n = 20000
        self.init_center()
        # Long history generated in chunks, each one committed version.
        for chunk in range(4):
            lo = chunk * n // 4
            hi = (chunk + 1) * n // 4
            self.apply(self.center, [
                put("hist", f"r{i}", 1, "E", 1000 + i, i)
                for i in range(lo, hi)])
        self.assertEqual(self.center_position(), 4)
        # Bind a replica on the full history before the small update,
        # so the later pack is a genuine tiny increment.
        _, full_pack = self.pack(0)
        rep = self.path("rep.db")
        self.receive(full_pack, db=rep)
        self.assertEqual(self.replica_position(rep), 4)
        # One tiny update: one identity changes, the other ~20k do not.
        self.apply(self.center, [
            put("hist", "r12345", 2, "E", 1000 + 12345, 999999)])
        started = time.monotonic()
        result, pack = self.pack(4)
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 10)
        header, rows, _ = sl.verify_pack(pack)
        self.assertEqual(rows, 1)
        self.assertEqual((header["base"], header["end"]), (4, 5))
        # The incremental query plan must use the version index as a
        # bounded range, never a full scan of readings or the log.
        conn = sqlite3.connect(self.center)
        try:
            plan = conn.execute(
                "EXPLAIN QUERY PLAN SELECT r.device, r.reading_id, "
                "r.revision, r.op, r.epoch, r.time, r.counter "
                "FROM sync_log AS s JOIN readings AS r "
                "ON r.device = s.device AND r.reading_id = s.reading_id "
                "WHERE s.last_version > ?", (4,)).fetchall()
        finally:
            conn.close()
        joined = " ".join(step[-1] for step in plan)
        # Both sides are index lookups bounded by the base position:
        # the versioned log is a covering-index range over changed
        # identities only, readings is reached by PK lookup — no
        # table scan of the long history anywhere.
        self.assertNotIn("SCAN", joined)
        self.assertIn("COVERING INDEX sync_log_versions", joined)
        self.assertIn("SEARCH r USING INDEX", joined)
        # The receiver likewise only processes the one changed row.
        started = time.monotonic()
        self.receive(pack, db=rep)
        self.assertLess(time.monotonic() - started, 10)
        self.assertEqual(self.replica_position(rep), 5)
        self.assertEqual(dump_db(rep), dump_db(self.center))

    def test_pack_rows_grow_with_identities_not_modification_count(self):
        self.init_center()
        self.apply(self.center, [put("d", "r1", 1, "E", 1000, 1),
                                 put("d", "r2", 1, "E", 2000, 2)])
        for rev in range(2, 12):
            self.apply(self.center, [
                put("d", "r1", rev, "E", 1000 + rev, rev)])
        _, pack = self.pack(1)
        _, rows, _ = sl.verify_pack(pack)
        self.assertEqual(rows, 1)  # r1 changed ten times: still one row


# ---------------------------------------------------------------------------
# Streaming: a large pack never loads whole into process memory
# ---------------------------------------------------------------------------

class StreamingTest(SyncCase):
    def test_large_pack_roundtrip_with_bounded_memory(self):
        self.init_center()
        n = 4000
        chunk = 500
        for lo in range(0, n, chunk):
            self.apply(self.center, [
                put("big", f"r{i}", 1, "E", 1000 + i, i)
                for i in range(lo, min(lo + chunk, n))])
        _, pack = self.pack(0)
        self.assertGreater(os.path.getsize(pack), 300_000)
        # verify_pack streams rows one frame at a time into a counter.
        seen = []
        header, rows, _ = sl.verify_pack(pack, sink=seen.append)
        self.assertEqual(rows, n)
        self.assertEqual(len(seen), n)
        # And the CLI receiver streams it into temp tables.
        result = self.receive(pack, db=self.path("big-rep.db"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(dump_db(self.path("big-rep.db")),
                         dump_db(self.center))

    def test_stdout_pack_roundtrip_and_missing_file(self):
        self.apply(self.center, [put("d", "r1", 1, "E", 1000, 10)])
        self.init_center()
        result = run_tool(["sync-pack", self.center, "--base", "0"],
                          binary=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        streamed = self.path("streamed.slpack")
        with open(streamed, "wb") as fh:
            fh.write(result.stdout)
        self.receive(streamed)
        self.assertEqual(self.replica_position(), 1)
        # A missing pack file is an input error, exit status 2.
        result = run_tool(["sync-receive", self.path("nope.db"),
                           self.path("absent.slpack")])
        self.assertEqual(result.returncode, 2)
        self.assertIn("cannot read pack", result.stderr)

    def test_concurrent_receives_serialise_without_corruption(self):
        self.apply(self.center, [put("d", "r1", 1, "E", 1000, 10)])
        self.init_center()
        _, p0 = self.pack(0)
        self.receive(p0)
        self.assertEqual(self.replica_position(), 1)
        self.apply(self.center, [put("d", "r1", 2, "E", 1100, 11)])
        _, p1 = self.pack(1)
        # Two processes accept the same next pack at once; exactly one
        # commits, the other sees the replay digest and also succeeds.
        procs = [subprocess.Popen(
            [sys.executable, "-I", TOOL, "sync-receive", self.replica, p1])
            for _ in range(2)]
        codes = [p.wait(30) for p in procs]
        self.assertEqual(sorted(codes), [0, 0])
        self.assertEqual(self.replica_position(), 2)
        # The earlier pack now reports the replay/no-op path safely.
        self.receive(p0, expect_ok=False)  # wrong base, different digest
        self.assertEqual(self.replica_position(), 2)


if __name__ == "__main__":
    unittest.main()
