#!/usr/bin/env python3
"""Tests for shift_ledger.py.

The oracle below is independent of the tool: for every check it recomputes
the effective readings from the full accepted message history and walks the
adjacent-point intervals shift by shift, straight from the specification.
Expected values in the narrative tests are hand-computed.

Run:  python3 -I test_shift_ledger.py
"""

import json
import os
import random
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import zlib

HERE = os.path.dirname(os.path.abspath(__file__))
TOOL = os.path.join(HERE, "shift_ledger.py")
REBUILD = os.path.join(HERE, "inspect_rebuild.py")
SHIFT = 28800


# ---------------------------------------------------------------------------
# Independent oracle: full recompute from the accepted messages
# ---------------------------------------------------------------------------

def effective_points(messages, device):
    """Sorted (time, epoch, counter) of the device's effective readings."""
    current = {}
    for m in messages:
        key = (m["device"], m["reading_id"])
        if key not in current or m["revision"] > current[key]["revision"]:
            current[key] = m
    return sorted((m["time"], m["epoch"], m["counter"])
                  for (dev, _), m in current.items()
                  if dev == device and m["op"] == "put")


def oracle_rows(messages, device, start, end):
    """One expected row per shift in [start, end), from the spec."""
    points = effective_points(messages, device)
    rows = []
    s = start
    while s < end:
        e = s + SHIFT
        covered = 0
        if points:
            covered = max(0, min(e, points[-1][0]) - max(s, points[0][0]))
        confirmed = 0
        uncertain = 0
        for (t1, e1, c1), (t2, e2, c2) in zip(points, points[1:]):
            lo, hi = max(t1, s), min(t2, e)
            if lo >= hi:
                continue
            if e1 == e2 and c2 >= c1:
                diff = c2 - c1
                if diff == 0:
                    continue  # zero difference confirms zero, even cross-shift
                if t1 >= s and t2 <= e:
                    confirmed += diff  # whole interval inside this shift
                else:
                    uncertain += hi - lo  # cross-shift: never apportioned
            else:
                uncertain += hi - lo  # epoch change or falling counter
        rows.append({
            "device": device,
            "start": s,
            "end": e,
            "confirmed_increment": confirmed,
            "covered_seconds": covered,
            "uncertain_seconds": uncertain,
            "exact": covered == SHIFT and uncertain == 0,
        })
        s = e
    return rows


def predict_accept(batch, accepted):
    """Independent model of the batch acceptance rules (spec only)."""
    current = {}
    for m in accepted:
        key = (m["device"], m["reading_id"])
        if key not in current or m["revision"] > current[key]["revision"]:
            current[key] = m

    def content(m):
        if m["op"] == "delete":
            return ("delete",)
        return ("put", m["epoch"], m["time"], m["counter"])

    bmax = {}
    for m in batch:
        key = (m["device"], m["reading_id"])
        bmax[key] = max(bmax.get(key, 0), m["revision"])
    for key, rev in bmax.items():
        contents = {content(m) for m in batch
                    if (m["device"], m["reading_id"]) == key
                    and m["revision"] == rev}
        if len(contents) > 1:
            return False  # highest version holds different messages
        if key in current and current[key]["revision"] == rev:
            if content(current[key]) != contents.pop():
                return False
    # simulate the winners and check the post-batch seconds
    merged = dict(current)
    for m in batch:
        key = (m["device"], m["reading_id"])
        if key not in merged or m["revision"] > merged[key]["revision"]:
            merged[key] = m
    seen = {}
    for (dev, rid), m in merged.items():
        if m["op"] != "put":
            continue
        other = seen.setdefault(dev, {})
        if m["time"] in other and other[m["time"]] != rid:
            return False
        other[m["time"]] = rid
    return True


# ---------------------------------------------------------------------------
# Running the tool
# ---------------------------------------------------------------------------

def run_tool(args, input_text=None):
    return subprocess.run([sys.executable, "-I", TOOL] + args,
                          capture_output=True, text=True, input=input_text)


def record_line(obj):
    return json.dumps({"type": "record", "start": 0, "end": 1,
                       "object": obj})


def put(device, rid, rev, epoch, t, counter):
    return {"device": device, "reading_id": rid, "revision": rev,
            "op": "put", "epoch": epoch, "time": t, "counter": counter}


def delete(device, rid, rev):
    return {"device": device, "reading_id": rid, "revision": rev,
            "op": "delete"}


class LedgerCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="shift-ledger-test-")
        self.db = os.path.join(self.dir, "ledger.db")
        self.accepted = []  # messages the oracle believes in
        self._n = 0

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    def write_batch(self, lines):
        """lines: message dicts (wrapped as record lines) or raw strings."""
        self._n += 1
        path = os.path.join(self.dir, f"batch{self._n}.jsonl")
        with open(path, "w", encoding="utf-8") as fh:
            for line in lines:
                if isinstance(line, str):
                    fh.write(line + "\n")
                else:
                    fh.write(record_line(line) + "\n")
        return path

    def apply(self, lines, expect_ok=True):
        path = self.write_batch(lines)
        result = run_tool(["apply", self.db, path])
        if expect_ok:
            self.assertEqual(result.returncode, 0,
                             f"apply failed: {result.stderr}")
            self.accepted += [l for l in lines if isinstance(l, dict)]
        else:
            self.assertNotEqual(result.returncode, 0,
                                "apply unexpectedly succeeded")
        return result

    def apply_file(self, path, expect_ok=True):
        result = run_tool(["apply", self.db, path])
        if expect_ok:
            self.assertEqual(result.returncode, 0,
                             f"apply failed: {result.stderr}")
        else:
            self.assertNotEqual(result.returncode, 0,
                                "apply unexpectedly succeeded")
        return result

    def query(self, device, start, end):
        result = run_tool(["query", self.db, device, str(start), str(end)])
        self.assertEqual(result.returncode, 0,
                         f"query failed: {result.stderr}")
        return [json.loads(x) for x in result.stdout.splitlines() if x.strip()]

    def check(self, device, start, end):
        got = self.query(device, start, end)
        want = oracle_rows(self.accepted, device, start, end)
        self.assertEqual(got, want)
        return got

    def summary_rows(self, table):
        conn = sqlite3.connect(self.db)
        try:
            return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        finally:
            conn.close()


# ---------------------------------------------------------------------------
# The narrative: split a cross-shift interval, move the point, retract it
# ---------------------------------------------------------------------------

class NarrativeTest(LedgerCase):
    def test_split_move_retract_replay(self):
        # A reliable positive difference across the 28800 boundary.
        self.apply([put("devA", "r1", 1, "E1", 28000, 100),
                    put("devA", "r2", 1, "E1", 30000, 250)])
        got = self.query("devA", 0, 3 * SHIFT)
        self.assertEqual([r["uncertain_seconds"] for r in got],
                         [800, 1200, 0])
        self.assertEqual([r["confirmed_increment"] for r in got], [0, 0, 0])
        self.assertEqual([r["covered_seconds"] for r in got],
                         [800, 1200, 0])
        self.assertEqual([r["exact"] for r in got], [False, False, False])

        # A reading exactly on the boundary splits the interval in two;
        # both halves now lie inside their own shift.
        self.apply([put("devA", "r3", 1, "E1", 28800, 180)])
        got = self.query("devA", 0, 3 * SHIFT)
        self.assertEqual([r["confirmed_increment"] for r in got],
                         [80, 70, 0])
        self.assertEqual([r["uncertain_seconds"] for r in got], [0, 0, 0])
        self.assertEqual([r["covered_seconds"] for r in got],
                         [800, 1200, 0])

        # The inserted point moves deeper into the second shift; the first
        # interval crosses the boundary again and turns uncertain there.
        self.apply([put("devA", "r3", 2, "E1", 29000, 185)])
        got = self.query("devA", 0, 3 * SHIFT)
        self.assertEqual([r["confirmed_increment"] for r in got], [0, 65, 0])
        self.assertEqual([r["uncertain_seconds"] for r in got],
                         [800, 200, 0])
        self.assertEqual([r["covered_seconds"] for r in got],
                         [800, 1200, 0])

        # Retracting the point restores the original cross-shift interval.
        self.apply([delete("devA", "r3", 3)])
        got = self.query("devA", 0, 3 * SHIFT)
        self.assertEqual([r["confirmed_increment"] for r in got], [0, 0, 0])
        self.assertEqual([r["uncertain_seconds"] for r in got],
                         [800, 1200, 0])

        # Replaying old messages changes nothing: the tombstone's revision
        # is still the highest for r3, and r1/r2 are identical replays.
        self.apply([put("devA", "r3", 2, "E1", 29000, 185),
                    put("devA", "r1", 1, "E1", 28000, 100),
                    put("devA", "r2", 1, "E1", 30000, 250)])
        got = self.query("devA", 0, 3 * SHIFT)
        self.assertEqual([r["uncertain_seconds"] for r in got],
                         [800, 1200, 0])
        self.assertEqual([r["confirmed_increment"] for r in got], [0, 0, 0])

        # Replaying r1's revision with a different counter is a conflict:
        # the whole batch is refused and the ledger does not move.
        result = self.apply([put("devA", "r1", 1, "E1", 28000, 101),
                             put("devA", "r9", 1, "E1", 40000, 500)],
                            expect_ok=False)
        self.assertEqual(result.returncode, 2)
        got = self.query("devA", 0, 3 * SHIFT)
        self.assertEqual([r["uncertain_seconds"] for r in got],
                         [800, 1200, 0])
        self.assertEqual([r["covered_seconds"] for r in got],
                         [800, 1200, 0])  # r9 never landed

    def test_boundary_reading_ends_previous_interval(self):
        # Readings exactly on shift boundaries: [28800, 57600) lies inside
        # shift 1; the boundary reading must not push it across.
        self.apply([put("devB", "a", 1, "E", 28800, 10),
                    put("devB", "b", 1, "E", 57600, 60)])
        got = self.query("devB", 0, 3 * SHIFT)
        self.assertEqual(got[0]["covered_seconds"], 0)
        self.assertEqual(got[1]["covered_seconds"], SHIFT)
        self.assertEqual(got[1]["confirmed_increment"], 50)
        self.assertEqual(got[1]["uncertain_seconds"], 0)
        self.assertTrue(got[1]["exact"])
        self.assertEqual(got[2]["covered_seconds"], 0)
        self.assertFalse(got[2]["exact"])

    def test_empty_and_unknown_shifts(self):
        self.apply([put("devC", "a", 1, "E", 1000, 0),
                    put("devC", "b", 1, "E", 2000, 10)])
        # Shifts with no readings at all are still emitted, not exact.
        got = self.query("devC", 2 * SHIFT, 4 * SHIFT)
        for row in got:
            self.assertEqual(row["covered_seconds"], 0)
            self.assertEqual(row["confirmed_increment"], 0)
            self.assertEqual(row["uncertain_seconds"], 0)
            self.assertFalse(row["exact"])
        # An unknown device must never look like an exact zero-output shift.
        got = self.query("ghost", 0, 2 * SHIFT)
        for row in got:
            self.assertEqual(row["covered_seconds"], 0)
            self.assertFalse(row["exact"])
        self.check("devC", 0, 5 * SHIFT)


# ---------------------------------------------------------------------------
# Zero difference across shifts vs epoch change
# ---------------------------------------------------------------------------

class IntervalKindTest(LedgerCase):
    def test_zero_diff_cross_shift_is_exact_zero(self):
        self.apply([put("z", "a", 1, "E", 1000, 500),
                    put("z", "b", 1, "E", 100000, 500)])
        got = self.query("z", 0, 5 * SHIFT)
        # Fully covered shifts confirm a clean zero.
        for row in got[1:3]:
            self.assertEqual(row["covered_seconds"], SHIFT)
            self.assertEqual(row["confirmed_increment"], 0)
            self.assertEqual(row["uncertain_seconds"], 0)
            self.assertTrue(row["exact"])
        self.assertEqual(got[0]["covered_seconds"], 27800)
        self.assertFalse(got[0]["exact"])  # only partly covered
        self.assertEqual(got[0]["uncertain_seconds"], 0)
        self.assertEqual(got[4]["covered_seconds"], 0)

    def test_epoch_change_is_uncertain(self):
        self.apply([put("e", "a", 1, "E1", 1000, 500),
                    put("e", "b", 1, "E2", 100000, 500)])
        got = self.query("e", 0, 5 * SHIFT)
        self.assertEqual(got[0]["uncertain_seconds"], 27800)
        for row in got[1:3]:
            self.assertEqual(row["uncertain_seconds"], SHIFT)
            self.assertEqual(row["covered_seconds"], SHIFT)
            self.assertFalse(row["exact"])
        self.assertEqual(got[3]["uncertain_seconds"], 13600)
        self.assertEqual(got[4]["uncertain_seconds"], 0)
        self.assertEqual(got[4]["covered_seconds"], 0)

    def test_counter_decrease_is_uncertain(self):
        self.apply([put("d", "a", 1, "E", 1000, 900),
                    put("d", "b", 1, "E", 2000, 100)])
        got = self.query("d", 0, SHIFT)
        self.assertEqual(got[0]["uncertain_seconds"], 1000)
        self.assertEqual(got[0]["confirmed_increment"], 0)

    def test_zero_diff_after_epoch_change_still_uncertain(self):
        # Same counter, different epoch: not a reliable zero.
        self.apply([put("dz", "a", 1, "E1", 1000, 500),
                    put("dz", "b", 1, "E2", 100000, 500)])
        got = self.query("dz", SHIFT, 3 * SHIFT)
        for row in got:
            self.assertEqual(row["uncertain_seconds"], SHIFT)
            self.assertFalse(row["exact"])

    def test_uncertain_interval_aligned_to_boundaries(self):
        # Endpoints exactly on shift boundaries: the spanned middle shift
        # and the two edge shifts each get a full shift of uncertain time,
        # with no double counting.
        self.apply([put("al", "a", 1, "E1", 28800, 100),
                    put("al", "b", 1, "E2", 115200, 100)])
        got = self.query("al", SHIFT, 5 * SHIFT)
        self.assertEqual([r["uncertain_seconds"] for r in got],
                         [SHIFT, SHIFT, SHIFT, 0])
        self.assertEqual([r["covered_seconds"] for r in got],
                         [SHIFT, SHIFT, SHIFT, 0])
        # Same shape with a reliable zero difference: nothing uncertain.
        self.apply([put("alz", "a", 1, "E", 28800, 100),
                    put("alz", "b", 1, "E", 115200, 100)])
        got = self.query("alz", SHIFT, 5 * SHIFT)
        self.assertEqual([r["uncertain_seconds"] for r in got],
                         [0, 0, 0, 0])
        self.assertEqual([r["exact"] for r in got],
                         [True, True, True, False])


# ---------------------------------------------------------------------------
# Version rules
# ---------------------------------------------------------------------------

class VersionTest(LedgerCase):
    def test_highest_revision_wins_and_lower_is_ignored(self):
        self.apply([put("v", "a", 5, "E", 1000, 100)])
        self.apply([put("v", "a", 3, "E", 5000, 999)])   # lower: no effect
        self.apply([put("v", "a", 7, "E", 2000, 300)])  # higher: moves
        self.apply([put("v", "b", 1, "E", 9000, 400)])
        self.check("v", 0, SHIFT)
        points = effective_points(self.accepted, "v")
        self.assertEqual(points, [(2000, "E", 300), (9000, "E", 400)])

    def test_delete_blocks_resurrection(self):
        self.apply([put("v", "a", 5, "E", 1000, 100),
                    put("v", "b", 1, "E", 8000, 500)])
        self.apply([delete("v", "a", 6)])
        self.apply([put("v", "a", 5, "E", 1000, 100)])  # older put: no effect
        self.apply([put("v", "a", 4, "E", 1000, 100)])  # even older: nothing
        points = effective_points(self.accepted, "v")
        self.assertEqual(points, [(8000, "E", 500)])
        self.check("v", 0, SHIFT)

    def test_same_revision_same_message_replays(self):
        self.apply([put("v", "a", 5, "E", 1000, 100)])
        self.apply([put("v", "a", 5, "E", 1000, 100)])  # identical replay
        self.apply([delete("v", "a", 6)])
        self.apply([delete("v", "a", 6)])               # identical replay
        self.check("v", 0, SHIFT)

    def test_same_revision_different_message_rejected(self):
        self.apply([put("v", "a", 5, "E", 1000, 100)])
        self.apply([put("v", "a", 5, "E", 1000, 101)], expect_ok=False)
        self.apply([delete("v", "a", 5)], expect_ok=False)
        self.apply([put("v", "a", 5, "E", 2000, 100)], expect_ok=False)
        self.check("v", 0, SHIFT)

    def test_conflict_inside_one_file(self):
        # Two different messages at the file's highest revision for one ID.
        self.apply([put("v", "a", 3, "E", 1000, 100),
                    put("v", "a", 3, "E", 1000, 101)], expect_ok=False)
        self.assertEqual(effective_points(self.accepted, "v"), [])
        # ... but a conflict below the ledger's version takes no part.
        self.apply([put("v", "a", 7, "E", 1000, 100)])
        self.apply([put("v", "a", 3, "E", 1000, 100),
                    put("v", "a", 3, "E", 2000, 101)])  # both below 7: no-op
        self.check("v", 0, SHIFT)

    def test_revision_bump_with_same_point_keeps_summaries(self):
        self.apply([put("v", "a", 1, "E", 1000, 100),
                    put("v", "b", 1, "E", 2000, 300)])
        before = self.query("v", 0, SHIFT)
        self.apply([put("v", "a", 2, "E", 1000, 100)])  # only revision moves
        after = self.query("v", 0, SHIFT)
        self.assertEqual(before, after)

    def test_delete_may_carry_ignored_fields(self):
        self.apply([put("v", "a", 1, "E", 1000, 100)])
        msg = delete("v", "a", 2)
        msg["epoch"] = "E"
        msg["time"] = 1000
        msg["counter"] = 100
        self.apply([msg])
        self.assertEqual(effective_points(self.accepted, "v"), [])
        self.check("v", 0, SHIFT)

    def test_several_revisions_of_one_id_in_one_file(self):
        # The file's highest revision wins; lower ones in the same file
        # take no part.
        self.apply([put("v", "a", 1, "E", 1000, 100),
                    put("v", "a", 3, "E", 2000, 300),
                    put("v", "a", 2, "E", 9000, 999),
                    put("v", "b", 1, "E", 4000, 10)])
        points = effective_points(self.accepted, "v")
        self.assertEqual(points, [(2000, "E", 300), (4000, "E", 10)])
        self.check("v", 0, SHIFT)
        # A lower revision inside a later file cannot move it back.
        self.apply([put("v", "a", 2, "E", 9000, 999)])
        self.assertEqual(effective_points(self.accepted, "v"),
                         [(2000, "E", 300), (4000, "E", 10)])

    def test_boundary_values(self):
        big_rev = 2 ** 63 - 1
        self.apply([put("v", "a", 1, "E", 0, 0),
                    put("v", "b", big_rev, "E", 2 ** 53 - 1, 10 ** 9)])
        self.check("v", 0, SHIFT)
        self.check("v", (2 ** 53 - 1) // SHIFT * SHIFT,
                   ((2 ** 53 - 1) // SHIFT + 1) * SHIFT)
        # Just outside the allowed ranges.
        self.apply([put("v", "c", 2 ** 63, "E", 0, 0)], expect_ok=False)
        self.apply([put("v", "c", 1, "E", 2 ** 53, 0)], expect_ok=False)
        self.apply([put("v", "c", 1, "E", 0, 10 ** 9 + 1)], expect_ok=False)

    def test_unicode_and_long_names(self):
        dev = "设备-α/β"
        self.apply([put(dev, "读数-1", 1, "纪元€", 1000, 1),
                    put(dev, "x" * 500, 1, "纪元€", 2000, 9)])
        got = self.query(dev, 0, SHIFT)
        self.assertEqual(got[0]["device"], dev)
        self.assertEqual(got[0]["confirmed_increment"], 8)
        self.check(dev, 0, SHIFT)

    def test_query_on_empty_database(self):
        # A database that does not exist yet behaves as an empty ledger.
        got = self.query("nobody", 0, 2 * SHIFT)
        for row in got:
            self.assertEqual(row["covered_seconds"], 0)
            self.assertFalse(row["exact"])


# ---------------------------------------------------------------------------
# Atomicity
# ---------------------------------------------------------------------------

class AtomicityTest(LedgerCase):
    def test_failed_batch_leaves_no_trace(self):
        self.apply([put("a", "r1", 1, "E", 28000, 100),
                    put("a", "r2", 1, "E", 30000, 250)])
        before = self.query("a", 0, 3 * SHIFT)
        # A batch whose valid parts would change summaries, plus a poison
        # pill at the end: everything must roll back.
        bad_variants = [
            # highest-version conflict: r1's current revision 1 replayed
            # with a different counter; the valid r9 must not survive either
            [put("a", "r9", 1, "E", 10000, 100),
             put("a", "r1", 1, "E", 28000, 101)],
            # final time conflict
            [put("a", "r1", 2, "E", 30000, 100)],
            # bad message (counter too large)
            [put("a", "r1", 2, "E", 10000, 100),
             {"device": "a", "reading_id": "r9", "revision": 1, "op": "put",
              "epoch": "E", "time": 1, "counter": 10 ** 9 + 1}],
            # bad JSONL
            ['{"type":"record","object":'],
            # record without object
            ['{"type":"record","start":0,"end":1}'],
            # not a JSON object
            ['[1,2,3]'],
        ]
        for i, bad in enumerate(bad_variants):
            with self.subTest(variant=i):
                self.apply(bad, expect_ok=False)
                self.assertEqual(self.query("a", 0, 3 * SHIFT), before)
        self.assertEqual(len(self.accepted), 2)

    def test_reopen_after_failure_shows_precommit_state(self):
        # Every CLI call is its own process; a failed apply in one process
        # must be invisible to a fresh process reopening the database.
        self.apply([put("a", "r1", 1, "E", 1000, 10),
                    put("a", "r2", 1, "E", 2000, 20)])
        before = self.query("a", 0, SHIFT)
        self.apply([put("a", "r1", 2, "E", 5000, 10),
                    put("a", "r3", 1, "E", 5000, 10)],  # time conflict
                   expect_ok=False)
        self.assertEqual(self.query("a", 0, SHIFT), before)
        self.apply([put("a", "r1", 2, "E", 5000, 10)])  # now it fits
        after = self.query("a", 0, SHIFT)
        self.assertNotEqual(after, before)
        self.check("a", 0, 2 * SHIFT)

    def test_reapplying_same_file_is_idempotent(self):
        batch = [put("a", "r1", 1, "E", 1000, 10),
                 put("a", "r2", 1, "E", 90000, 10),
                 delete("a", "r3", 1)]
        path = self.write_batch(batch)
        self.accepted += batch
        self.assertEqual(run_tool(["apply", self.db, path]).returncode, 0)
        first = self.query("a", 0, 4 * SHIFT)
        self.assertEqual(run_tool(["apply", self.db, path]).returncode, 0)
        self.assertEqual(self.query("a", 0, 4 * SHIFT), first)


# ---------------------------------------------------------------------------
# Time conflicts and same-batch exchanges
# ---------------------------------------------------------------------------

class TimeConflictTest(LedgerCase):
    def test_two_ids_one_second_rejected(self):
        self.apply([put("t", "a", 1, "E", 1000, 1),
                    put("t", "b", 1, "E", 1000, 2)], expect_ok=False)
        self.assertEqual(effective_points(self.accepted, "t"), [])

    def test_same_batch_time_exchange_is_legal(self):
        self.apply([put("t", "a", 1, "E", 1000, 100),
                    put("t", "b", 1, "E", 2000, 200),
                    put("t", "c", 1, "E", 3000, 300)])
        # a and b swap seconds inside one batch: only the post-batch state
        # is judged, so this is legal.
        self.apply([put("t", "a", 2, "E", 2000, 100),
                    put("t", "b", 2, "E", 1000, 200)])
        points = effective_points(self.accepted, "t")
        self.assertEqual(points, [(1000, "E", 200), (2000, "E", 100),
                                  (3000, "E", 300)])
        self.check("t", 0, SHIFT)

    def test_placeholder_replaced_in_one_batch(self):
        self.apply([put("t", "hold", 1, "E", 5000, 50),
                    put("t", "x", 1, "E", 1000, 10)])
        # The placeholder is retracted and its second reused in one batch.
        self.apply([delete("t", "hold", 2),
                    put("t", "y", 1, "E", 5000, 60)])
        points = effective_points(self.accepted, "t")
        self.assertEqual(points, [(1000, "E", 10), (5000, "E", 60)])
        self.check("t", 0, SHIFT)

    def test_move_onto_occupied_second_rejected(self):
        self.apply([put("t", "a", 1, "E", 1000, 1),
                    put("t", "b", 1, "E", 2000, 2)])
        self.apply([put("t", "a", 2, "E", 2000, 1)], expect_ok=False)
        points = effective_points(self.accepted, "t")
        self.assertEqual(points, [(1000, "E", 1), (2000, "E", 2)])
        self.check("t", 0, SHIFT)

    def test_same_second_on_different_devices_is_fine(self):
        self.apply([put("t1", "a", 1, "E", 1000, 1),
                    put("t2", "a", 1, "E", 1000, 1)])
        self.check("t1", 0, SHIFT)
        self.check("t2", 0, SHIFT)


# ---------------------------------------------------------------------------
# Input line handling
# ---------------------------------------------------------------------------

class LineHandlingTest(LedgerCase):
    def test_only_record_lines_produce_readings(self):
        lines = [
            put("L", "a", 1, "E", 1000, 10),
            json.dumps({"type": "missing", "start": 1, "end": 2}),
            json.dumps({"type": "conflict", "start": 2, "end": 3}),
            json.dumps({"type": "invalid", "start": 3, "end": 4}),
            json.dumps({"type": "boundary_unknown", "start": 4, "end": 5}),
            put("L", "b", 1, "E", 2000, 20),
            "",   # blank lines carry nothing
        ]
        self.apply(lines)
        # The non-record lines must not fabricate readings or devices.
        self.assertEqual(effective_points(self.accepted, "L"),
                         [(1000, "E", 10), (2000, "E", 20)])
        self.check("L", 0, SHIFT)
        self.check("missing", 0, SHIFT)  # unknown device: empty, not exact

    def test_bad_lines_rejected(self):
        bad = [
            '{"type":"record"',                      # broken JSON
            '12345',                                 # not an object
            '{"start":0}',                           # missing type
            '{"type":7}',                            # type not a string
            record_line({"device": "", "reading_id": "a", "revision": 1,
                         "op": "delete"}),           # empty device
            record_line({"device": "d", "reading_id": "", "revision": 1,
                         "op": "delete"}),           # empty id
            record_line({"device": "d", "reading_id": "a", "revision": 0,
                         "op": "delete"}),           # revision too small
            record_line({"device": "d", "reading_id": "a",
                         "revision": 2 ** 63, "op": "delete"}),
            record_line({"device": "d", "reading_id": "a", "revision": 1.5,
                         "op": "delete"}),           # not an integer
            record_line({"device": "d", "reading_id": "a", "revision": True,
                         "op": "delete"}),           # bool is not an integer
            record_line({"device": "d", "reading_id": "a", "revision": 1,
                         "op": "put", "epoch": "", "time": 0, "counter": 0}),
            record_line({"device": "d", "reading_id": "a", "revision": 1,
                         "op": "put", "epoch": "E", "time": -1,
                         "counter": 0}),
            record_line({"device": "d", "reading_id": "a", "revision": 1,
                         "op": "put", "epoch": "E", "time": 2 ** 53,
                         "counter": 0}),
            record_line({"device": "d", "reading_id": "a", "revision": 1,
                         "op": "put", "epoch": "E", "time": 0,
                         "counter": -1}),
            record_line({"device": "d", "reading_id": "a", "revision": 1,
                         "op": "put", "epoch": "E", "time": 0,
                         "counter": 10 ** 9 + 1}),
            record_line({"device": "d", "reading_id": "a", "revision": 1,
                         "op": "move"}),
            record_line({"device": "d", "reading_id": "a", "revision": 1,
                         "op": "put", "epoch": "E", "time": 0}),
            record_line("not an object"),
        ]
        for i, line in enumerate(bad):
            with self.subTest(line=i):
                self.apply([line], expect_ok=False)
        self.assertEqual(self.accepted, [])

    def test_apply_from_stdin(self):
        text = "\n".join([record_line(put("S", "a", 1, "E", 1000, 1)),
                          record_line(put("S", "b", 1, "E", 2000, 5))])
        result = run_tool(["apply", self.db, "-"], input_text=text)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.accepted += [put("S", "a", 1, "E", 1000, 1),
                          put("S", "b", 1, "E", 2000, 5)]
        self.check("S", 0, SHIFT)


# ---------------------------------------------------------------------------
# Query command line
# ---------------------------------------------------------------------------

class QueryCommandTest(LedgerCase):
    def test_range_validation(self):
        self.apply([put("q", "a", 1, "E", 1000, 1)])
        for args in (["query", self.db, "q", "100", "28800"],      # not aligned
                     ["query", self.db, "q", "0", "28801"],        # not aligned
                     ["query", self.db, "q", "28800", "28800"],    # empty range
                     ["query", self.db, "q", "57600", "28800"],    # backwards
                     ["query", self.db, "q", "-28800", "0"]):      # negative
            with self.subTest(args=args):
                result = run_tool(list(args))
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")

    def test_output_file_is_atomic(self):
        self.apply([put("q", "a", 1, "E", 1000, 1),
                    put("q", "b", 1, "E", 2000, 4)])
        out = os.path.join(self.dir, "out.jsonl")
        result = run_tool(["query", self.db, "q", "0", "57600", "-o", out])
        self.assertEqual(result.returncode, 0, result.stderr)
        with open(out, encoding="utf-8") as fh:
            rows = [json.loads(x) for x in fh if x.strip()]
        self.assertEqual(rows, oracle_rows(self.accepted, "q", 0, 57600))
        # A failed query must not create or replace the file.
        with open(out, "w", encoding="utf-8") as fh:
            fh.write("keep me\n")
        result = run_tool(["query", self.db, "q", "1", "57600", "-o", out])
        self.assertEqual(result.returncode, 2)
        with open(out, encoding="utf-8") as fh:
            self.assertEqual(fh.read(), "keep me\n")


# ---------------------------------------------------------------------------
# Density and long gaps: no scanning, no per-shift expansion
# ---------------------------------------------------------------------------

class ScaleTest(LedgerCase):
    def test_dense_shift(self):
        base = 5 * SHIFT
        n = 12000
        files = []
        for chunk in range(3):
            lines = []
            for i in range(chunk * n // 3, (chunk + 1) * n // 3):
                m = put("dense", f"d{i}", 1, "E", base + i, 1000 + i)
                lines.append(m)
                self.accepted.append(m)
            files.append(self.write_batch(lines))
        for path in files:
            self.assertEqual(
                run_tool(["apply", self.db, path]).returncode, 0)
        # The whole shift collapses to a single summary row.
        self.assertEqual(self.summary_rows("confirmed"), 1)
        self.assertEqual(self.summary_rows("uncertain_edge"), 0)
        started = time.monotonic()
        got = self.query("dense", 4 * SHIFT, 7 * SHIFT)
        elapsed = time.monotonic() - started
        self.assertEqual(got, oracle_rows(self.accepted, "dense",
                                          4 * SHIFT, 7 * SHIFT))
        self.assertEqual(got[1]["confirmed_increment"], n - 1)
        self.assertEqual(got[1]["covered_seconds"], n - 1)
        self.assertLess(elapsed, 10,
                        "querying a few shifts must not walk dense readings")
        # Moving one dense point touches only its own adjacency.
        self.apply([put("dense", "d6000", 2, "E", base + 6000, 96000)])
        self.check("dense", 4 * SHIFT, 7 * SHIFT)

    def test_far_gap_is_not_expanded_per_shift(self):
        t0 = 1000
        t1 = t0 + 10 * 365 * 86400  # ten years, ~11k shifts away
        self.apply([put("gap", "a", 1, "E", t0, 500),
                    put("gap", "b", 1, "E", t1, 100)])  # falling: uncertain
        # Only the two edge shifts may exist as rows; the ~11k spanned
        # shifts must not be materialised.
        self.assertEqual(self.summary_rows("uncertain_edge"), 2)
        self.assertEqual(self.summary_rows("confirmed"), 0)
        # Spot-check shifts across the gap against the oracle.
        for shift in (0, 1, 5000, 10949, 10950):
            s = shift * SHIFT
            self.assertEqual(self.query("gap", s, s + SHIFT),
                             oracle_rows(self.accepted, "gap", s, s + SHIFT))
        middle = self.query("gap", 5000 * SHIFT, 5001 * SHIFT)[0]
        self.assertEqual(middle["uncertain_seconds"], SHIFT)
        self.assertEqual(middle["covered_seconds"], SHIFT)
        self.assertFalse(middle["exact"])
        # Moving the far endpoint still touches nothing in between.
        t2 = t1 + 5 * 365 * 86400
        self.apply([put("gap", "b", 2, "E", t2, 100)])
        self.assertEqual(self.summary_rows("uncertain_edge"), 2)
        self.check("gap", 7000 * SHIFT, 7002 * SHIFT)
        # Retracting it leaves no residue in the summary tables.
        self.apply([delete("gap", "b", 3)])
        self.assertEqual(self.summary_rows("uncertain_edge"), 0)
        self.check("gap", 0, 3 * SHIFT)
        self.check("gap", 5000 * SHIFT, 5001 * SHIFT)

    def test_many_empty_shifts_stream_out(self):
        self.apply([put("s", "a", 1, "E", 0, 0),
                    put("s", "b", 1, "E", SHIFT, 7)])
        got = self.query("s", 0, 400 * SHIFT)
        self.assertEqual(len(got), 400)
        self.assertEqual(got, oracle_rows(self.accepted, "s", 0, 400 * SHIFT))


# ---------------------------------------------------------------------------
# Pipeline with the real inspect_rebuild.py output
# ---------------------------------------------------------------------------

class RebuildIntegrationTest(LedgerCase):
    def test_end_to_end_through_rebuild(self):
        messages = [put("pipe", "r1", 1, "E1", 28000, 100),
                    put("pipe", "r2", 1, "E1", 30000, 250),
                    put("pipe", "r3", 1, "E1", 28800, 180)]
        records = []
        for m in messages:
            payload = json.dumps(m, separators=(",", ":")).encode("utf-8")
            records.append(len(payload).to_bytes(4, "little") + payload
                           + (zlib.crc32(payload) & 0xFFFFFFFF)
                           .to_bytes(4, "little"))
        stream = b"".join(records)
        # Cut the stream into fragments with a hole inside the third record.
        cut1 = len(records[0])
        cut2 = cut1 + len(records[1])
        hole = (cut2 + 5, cut2 + 9)
        fragments = [
            (0, stream[:cut1], "f0.bin"),
            (cut1, stream[cut1:hole[0]], "f1.bin"),
            (hole[1], stream[hole[1]:], "f2.bin"),
        ]
        for _, data, name in fragments:
            with open(os.path.join(self.dir, name), "wb") as fh:
                fh.write(data)
        catalog = {"total_size": len(stream), "sync_points": [0],
                   "fragments": [{"path": name, "start": start,
                                  "length": len(data)}
                                 for start, data, name in fragments]}
        catalog_path = os.path.join(self.dir, "catalog.json")
        with open(catalog_path, "w", encoding="utf-8") as fh:
            json.dump(catalog, fh)
        jsonl = os.path.join(self.dir, "rebuilt.jsonl")
        result = subprocess.run([sys.executable, "-I", REBUILD, catalog_path,
                                 "-o", jsonl], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        # The rebuild emits two records plus one missing line; only the two
        # records may become readings.
        self.assertEqual(run_tool(["apply", self.db, jsonl]).returncode, 0)
        self.accepted += messages[:2]
        self.check("pipe", 0, 3 * SHIFT)
        got = self.query("pipe", 0, 2 * SHIFT)
        self.assertEqual([r["uncertain_seconds"] for r in got], [800, 1200])


# ---------------------------------------------------------------------------
# Randomized differential test against the oracle
# ---------------------------------------------------------------------------

class RandomizedTest(LedgerCase):
    def test_random_batches(self):
        rng = random.Random(20261122)
        devices = ["devA", "devB"]
        epochs = ["E1", "E2", "E3"]
        ids = [f"r{i}" for i in range(10)]
        live = {}
        rejected = 0
        for bi in range(60):
            batch = []
            used = set()
            roll = rng.random()
            if roll < 0.10 and bi > 0:
                # Deliberate same-second collision between two IDs.
                dev = rng.choice(devices)
                t = rng.randint(0, 400000)
                batch = [put(dev, "x1", live.get((dev, "x1"), 0) + 1,
                             "E1", t, 1),
                         put(dev, "x2", live.get((dev, "x2"), 0) + 1,
                             "E1", t, 2)]
            elif roll < 0.18 and bi > 0:
                # Deliberate same-revision conflict with the ledger.
                dev = rng.choice(devices)
                rid = rng.choice(ids)
                rev = live.get((dev, rid), 0)
                if rev:
                    batch = [put(dev, rid, rev, "E1", 12345, 1),
                             put(dev, rid, rev, "E2", 54321, 2)]
            if not batch:
                for _ in range(rng.randint(1, 8)):
                    dev = rng.choice(devices)
                    rid = rng.choice(ids)
                    key = (dev, rid)
                    if key in used:
                        continue
                    used.add(key)
                    roll = rng.random()
                    rev = live.get(key, 0)
                    if roll < 0.08 and rev:
                        # identical replay of the current version
                        old = next(m for m in reversed(self.accepted)
                                   if (m["device"], m["reading_id"]) == key
                                   and m["revision"] == rev)
                        batch.append(dict(old))
                        continue
                    rev += rng.choice([1, 1, 2])
                    if roll < 0.25:
                        m = delete(dev, rid, rev)
                    else:
                        m = put(dev, rid, rev, rng.choice(epochs),
                                rng.randint(0, 400000), rng.randint(0, 10 ** 9))
                    batch.append(m)
            if predict_accept(batch, self.accepted):
                self.apply(batch)
                for m in batch:
                    key = (m["device"], m["reading_id"])
                    live[key] = max(live.get(key, 0), m["revision"])
            else:
                rejected += 1
                self.apply(batch, expect_ok=False)
            for _ in range(2):
                dev = rng.choice(devices + ["ghost"])
                s = rng.randint(0, 13) * SHIFT
                e = s + rng.randint(1, 6) * SHIFT
                self.check(dev, s, e)
        self.assertGreater(rejected, 0, "expected some rejected batches")
        # Final full-range comparison for every device.
        self.check("devA", 0, 14 * SHIFT)
        self.check("devB", 0, 14 * SHIFT)


# ---------------------------------------------------------------------------
# Settle export: independent oracle
# ---------------------------------------------------------------------------

def oracle_settle(messages, assignments, devices, t_from, t_to):
    """Expected manifest, recomputed from the spec: effective readings
    from the full accepted message history, job coverage evaluated
    second by second."""
    current = {}
    for m in messages:
        key = (m["device"], m["reading_id"])
        if key not in current or m["revision"] > current[key]["revision"]:
            current[key] = m
    lines = []
    for device in sorted(set(devices)):
        points = sorted(
            (m["time"], m["epoch"], m["counter"], m["reading_id"],
             m["revision"])
            for (dev, _), m in current.items()
            if dev == device and m["op"] == "put")
        assigns = [a for a in assignments if a["device"] == device]
        for left, right in zip(points, points[1:]):
            t1, e1, c1, id1, rev1 = left
            t2, e2, c2, id2, rev2 = right
            if not (t_from <= t2 < t_to):
                continue
            row = {"device": device, "start": t1, "end": t2,
                   "left_id": id1, "left_revision": rev1,
                   "right_id": id2, "right_revision": rev2}
            if e1 != e2 or c2 < c1:
                lines.append({**row, "status": "unmeasurable",
                              "job": None, "amount": None})
                continue
            diff = c2 - c1
            if diff == 0:
                continue
            overlap = False
            uncovered = False
            first_job = None
            changed = False
            for t in range(t1, t2):
                jobs = {a["job"] for a in assigns
                        if a["start"] <= t < a["end"]}
                if len(jobs) >= 2:
                    overlap = True
                    break
                if not jobs:
                    uncovered = True
                elif first_job is None:
                    first_job = next(iter(jobs))
                elif next(iter(jobs)) != first_job:
                    changed = True
            if overlap:
                row.update(status="overlap", job=None, amount=diff)
            elif uncovered:
                row.update(status="uncovered", job=None, amount=diff)
            elif changed:
                row.update(status="handoff", job=None, amount=diff)
            else:
                row.update(status="allocated", job=first_job, amount=diff)
            lines.append(row)
    return lines


def assignment(device, job, start, end):
    return {"device": device, "job": job, "start": start, "end": end}


class SettleCase(LedgerCase):
    def setUp(self):
        super().setUp()
        self._na = 0

    def write_assignments(self, lines):
        """lines: assignment dicts or raw strings."""
        self._na += 1
        path = os.path.join(self.dir, f"assign{self._na}.jsonl")
        with open(path, "w", encoding="utf-8") as fh:
            for line in lines:
                if isinstance(line, str):
                    fh.write(line + "\n")
                else:
                    fh.write(json.dumps(line) + "\n")
        return path

    def settle(self, devices, t_from, t_to, assign_path, output="-",
               extra_args=()):
        args = ["settle", self.db, "--from", str(t_from),
                "--to", str(t_to), "--assign", assign_path,
                "-o", output] + list(extra_args) + list(devices)
        return run_tool(args)

    def settle_rows(self, devices, t_from, t_to, assign_path):
        result = self.settle(devices, t_from, t_to, assign_path)
        self.assertEqual(result.returncode, 0,
                         f"settle failed: {result.stderr}")
        return [json.loads(x) for x in result.stdout.splitlines()
                if x.strip()]

    def check_settle(self, devices, t_from, t_to, assignments):
        path = self.write_assignments(assignments)
        got = self.settle_rows(devices, t_from, t_to, path)
        want = oracle_settle(self.accepted, assignments, devices,
                             t_from, t_to)
        self.assertEqual(got, want)
        return got


# ---------------------------------------------------------------------------
# The narrative: hand-derived assignment timelines
# ---------------------------------------------------------------------------

class SettleNarrativeTest(SettleCase):
    def test_same_job_rows_cover_then_real_gap(self):
        self.apply([put("A", "r1", 1, "E", 100, 0),
                    put("A", "r2", 1, "E", 200, 50)])
        # Two overlapping rows of the SAME job, 70 + 60 = 130 seconds
        # in total; every second of [100, 200) sees exactly job J.
        path = self.write_assignments([assignment("A", "J", 90, 160),
                                       assignment("A", "J", 150, 210)])
        got = self.settle_rows(["A"], 0, 1000, path)
        self.assertEqual(got, [{"device": "A", "start": 100, "end": 200,
                                "left_id": "r1", "left_revision": 1,
                                "right_id": "r2", "right_revision": 1,
                                "status": "allocated", "job": "J",
                                "amount": 50}])
        # Same total length (65 + 65 = 130) but a real gap [155, 165).
        path = self.write_assignments([assignment("A", "J", 90, 155),
                                       assignment("A", "J", 165, 230)])
        got = self.settle_rows(["A"], 0, 1000, path)
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["status"], "uncovered")
        self.assertIsNone(got[0]["job"])
        self.assertEqual(got[0]["amount"], 50)

    def test_abutting_jobs_are_handoff(self):
        self.apply([put("B", "r1", 1, "E", 100, 0),
                    put("B", "r2", 1, "E", 200, 30)])
        # J1 hands over to J2 at exactly second 150: no gap, no overlap.
        path = self.write_assignments([assignment("B", "J1", 100, 150),
                                       assignment("B", "J2", 150, 200)])
        got = self.settle_rows(["B"], 0, 1000, path)
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["status"], "handoff")
        self.assertIsNone(got[0]["job"])
        self.assertEqual(got[0]["amount"], 30)

    def test_overlap_outranks_uncovered(self):
        self.apply([put("C", "r1", 1, "E", 100, 0),
                    put("C", "r2", 1, "E", 300, 70)])
        # Overlap on [150, 160) AND a bare stretch on [200, 300):
        # one overlapping second decides the whole interval.
        path = self.write_assignments([assignment("C", "J1", 100, 160),
                                       assignment("C", "J2", 150, 200)])
        got = self.settle_rows(["C"], 0, 1000, path)
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["status"], "overlap")
        self.assertIsNone(got[0]["job"])
        self.assertEqual(got[0]["amount"], 70)

    def test_exact_duplicate_rows_count_once(self):
        self.apply([put("D", "r1", 1, "E", 100, 0),
                    put("D", "r2", 1, "E", 200, 8)])
        row = assignment("D", "J", 50, 250)
        path = self.write_assignments([row, dict(row), dict(row)])
        got = self.settle_rows(["D"], 0, 1000, path)
        self.assertEqual([r["status"] for r in got], ["allocated"])
        self.assertEqual(got[0]["amount"], 8)


# ---------------------------------------------------------------------------
# Adjacent export windows see the same whole intervals
# ---------------------------------------------------------------------------

class SettleWindowTest(SettleCase):
    def test_interval_is_never_cut_at_window_edges(self):
        self.apply([put("W", "r1", 1, "E", 100, 10),
                    put("W", "r2", 1, "E", 400, 60)])
        path = self.write_assignments([assignment("W", "J", 0, 1000)])
        # The interval's right end (400) is outside [0, 250): no line,
        # and the left end must NOT be fabricated at the range start.
        self.assertEqual(self.settle_rows(["W"], 0, 250, path), [])
        # [250, 500): the whole interval [100, 400) appears, with its
        # true left reading that lies before the window.
        got = self.settle_rows(["W"], 250, 500, path)
        self.assertEqual(got, [{"device": "W", "start": 100, "end": 400,
                                "left_id": "r1", "left_revision": 1,
                                "right_id": "r2", "right_revision": 1,
                                "status": "allocated", "job": "J",
                                "amount": 50}])

    def test_adjacent_windows_partition_the_chain(self):
        self.apply([put("W2", "r1", 1, "E", 100, 0),
                    put("W2", "r2", 1, "E", 250, 20),
                    put("W2", "r3", 1, "E", 400, 45)])
        path = self.write_assignments([assignment("W2", "J", 0, 1000)])
        first = self.settle_rows(["W2"], 0, 300, path)
        second = self.settle_rows(["W2"], 300, 500, path)
        self.assertEqual([(r["start"], r["end"], r["amount"])
                          for r in first], [(100, 250, 20)])
        self.assertEqual([(r["start"], r["end"], r["amount"])
                          for r in second], [(250, 400, 25)])
        # Together the two windows account for each interval once,
        # and the pieces still chain.
        self.assertEqual(first[-1]["end"], second[0]["start"])

    def test_readings_exactly_on_range_bounds(self):
        self.apply([put("W3", "r0", 1, "E", 50, 0),
                    put("W3", "r1", 1, "E", 100, 5),
                    put("W3", "r2", 1, "E", 200, 15),
                    put("W3", "r3", 1, "E", 300, 30)])
        path = self.write_assignments([assignment("W3", "J", 0, 1000)])
        # A reading exactly AT `from` closes an interval owned by this
        # window; a reading exactly AT `to` belongs to the next one.
        # [100, 300): right ends 100 and 200 are in, 300 is not.
        got = self.settle_rows(["W3"], 100, 300, path)
        self.assertEqual([(r["start"], r["end"]) for r in got],
                         [(50, 100), (100, 200)])
        # [0, 100): the only reading inside (50) ends no interval.
        self.assertEqual(self.settle_rows(["W3"], 0, 100, path), [])
        # [0, 101): now right end 100 qualifies -> [50, 100).
        got = self.settle_rows(["W3"], 0, 101, path)
        self.assertEqual([(r["start"], r["end"]) for r in got],
                         [(50, 100)])
        # [300, 400): right end 300 -> [200, 300), left end fetched as
        # the true predecessor before the window.
        got = self.settle_rows(["W3"], 300, 400, path)
        self.assertEqual([(r["start"], r["end"]) for r in got],
                         [(200, 300)])


# ---------------------------------------------------------------------------
# Zero difference, counter reset, epoch change, corrected evidence
# ---------------------------------------------------------------------------

class SettleReadingTest(SettleCase):
    def test_zero_reset_epoch_and_correction(self):
        self.apply([put("E", "r1", 1, "E1", 100, 500),
                    put("E", "r2", 1, "E1", 200, 500),   # zero diff
                    put("E", "r3", 1, "E1", 300, 450),   # falling
                    put("E", "r4", 1, "E2", 400, 10)])   # epoch change
        path = self.write_assignments([assignment("E", "J", 0, 1000)])
        got = self.settle_rows(["E"], 0, 1000, path)
        # [100,200) is a reliable zero: no line at all.  The other two
        # intervals are unmeasurable even though J covers them fully.
        self.assertEqual(
            [(r["start"], r["status"], r["job"], r["amount"]) for r in got],
            [(200, "unmeasurable", None, None),
             (300, "unmeasurable", None, None)])
        # Correct r2's counter upward at a higher revision: [100, 200)
        # becomes settleable and the line cites the corrected evidence.
        self.apply([put("E", "r2", 2, "E1", 200, 530)])
        got = self.settle_rows(["E"], 0, 1000, path)
        self.assertEqual(
            [(r["start"], r["status"], r["amount"]) for r in got],
            [(100, "allocated", 30),
             (200, "unmeasurable", None),
             (300, "unmeasurable", None)])
        self.assertEqual((got[0]["left_id"], got[0]["left_revision"]),
                         ("r1", 1))
        self.assertEqual((got[0]["right_id"], got[0]["right_revision"]),
                         ("r2", 2))
        # Move r3 to a new second at a higher revision: the intervals
        # reshape and every line cites the winning versions.
        self.apply([put("E", "r3", 2, "E1", 250, 450)])
        got = self.settle_rows(["E"], 0, 1000, path)
        self.assertEqual(
            [(r["start"], r["end"], r["left_id"], r["left_revision"],
              r["right_id"], r["right_revision"], r["status"])
             for r in got],
            [(100, 200, "r1", 1, "r2", 2, "allocated"),
             (200, 250, "r2", 2, "r3", 2, "unmeasurable"),
             (250, 400, "r3", 2, "r4", 1, "unmeasurable")])

    def test_unmeasurable_ignores_assignments(self):
        self.apply([put("U", "r1", 1, "E1", 100, 50),
                    put("U", "r2", 1, "E2", 200, 50)])  # epoch change
        # The assignment situation would say "overlap"; the readings
        # themselves win.
        path = self.write_assignments([assignment("U", "J1", 100, 150),
                                       assignment("U", "J2", 140, 200)])
        got = self.settle_rows(["U"], 0, 1000, path)
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["status"], "unmeasurable")
        self.assertIsNone(got[0]["job"])
        self.assertIsNone(got[0]["amount"])

    def test_zero_interval_between_positive_ones(self):
        self.apply([put("Z", "r1", 1, "E", 100, 10),
                    put("Z", "r2", 1, "E", 200, 20),
                    put("Z", "r3", 1, "E", 300, 20),   # zero: no line
                    put("Z", "r4", 1, "E", 400, 35)])
        # Jobs change across the silent interval; the sweep must still
        # line up the later interval with its own facts.
        path = self.write_assignments([assignment("Z", "J1", 100, 200),
                                       assignment("Z", "J2", 300, 400)])
        got = self.settle_rows(["Z"], 0, 1000, path)
        self.assertEqual(
            [(r["start"], r["status"], r["job"], r["amount"]) for r in got],
            [(100, "allocated", "J1", 10),
             (300, "allocated", "J2", 15)])

    def test_assignment_edges_are_half_open(self):
        self.apply([put("H", "r1", 1, "E", 100, 0),
                    put("H", "r2", 1, "E", 200, 9)])
        cases = [
            # ends exactly at the interval start: covers nothing
            ([assignment("H", "J", 0, 100)], "uncovered"),
            # starts exactly at the interval end: covers nothing
            ([assignment("H", "J", 200, 300)], "uncovered"),
            # the two together still leave the whole interval bare
            ([assignment("H", "J1", 0, 100),
              assignment("H", "J2", 200, 300)], "uncovered"),
            # exact fit
            ([assignment("H", "J", 100, 200)], "allocated"),
            # covers only the first half
            ([assignment("H", "J", 50, 150)], "uncovered"),
        ]
        for i, (rows, want) in enumerate(cases):
            with self.subTest(case=i):
                path = self.write_assignments(rows)
                got = self.settle_rows(["H"], 0, 1000, path)
                self.assertEqual([r["status"] for r in got], [want])


# ---------------------------------------------------------------------------
# Ordering, deduplication, range bounds
# ---------------------------------------------------------------------------

class SettleOrderingTest(SettleCase):
    def test_device_code_point_order_and_time_order(self):
        # Code points: "A" 65 < "b" 98 < "z" 122 < "é" 233.
        for dev in ("z", "A", "é", "b"):
            self.apply([put(dev, "r1", 1, "E", 100, 0),
                        put(dev, "r2", 1, "E", 150, 5),
                        put(dev, "r3", 1, "E", 190, 25)])
        path = self.write_assignments(
            [assignment(d, "J", 0, 1000) for d in ("z", "A", "é", "b")])
        # "b" is named twice: it must be settled once.
        got = self.settle_rows(["é", "z", "b", "A", "b"], 0, 1000, path)
        self.assertEqual([r["device"] for r in got],
                         ["A", "A", "b", "b", "z", "z", "é", "é"])
        for i in range(4):
            pair = got[2 * i:2 * i + 2]
            self.assertEqual([(r["start"], r["end"]) for r in pair],
                             [(100, 150), (150, 190)])
            self.assertEqual([r["amount"] for r in pair], [5, 20])

    def test_latest_reading_is_reachable(self):
        big = 2 ** 53 - 1
        self.apply([put("T", "r1", 1, "E", big - 10, 0),
                    put("T", "r2", 1, "E", big, 7)])
        path = self.write_assignments(
            [assignment("T", "J", big - 10, big)])
        # TO may be 2^53 exactly, which selects the latest reading.
        got = self.settle_rows(["T"], big - 10, 2 ** 53, path)
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["status"], "allocated")
        self.assertEqual(got[0]["amount"], 7)
        # TO = big excludes the reading at big: no interval ends inside.
        self.assertEqual(self.settle_rows(["T"], big - 10, big, path), [])
        # FROM = big: r1 is only the predecessor, r2 is the right end.
        got = self.settle_rows(["T"], big, 2 ** 53, path)
        self.assertEqual([(r["start"], r["end"]) for r in got],
                         [(big - 10, big)])

    def test_bad_ranges_and_device_lists(self):
        path = self.write_assignments([assignment("A", "J", 0, 10)])
        for t_from, t_to in ((100, 100), (200, 100), (-1, 100),
                             (0, 2 ** 53 + 1)):
            with self.subTest(range=(t_from, t_to)):
                result = self.settle(["A"], t_from, t_to, path)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, "")
        # no device at all
        result = run_tool(["settle", self.db, "--from", "0", "--to", "1",
                           "--assign", path])
        self.assertEqual(result.returncode, 2)


# ---------------------------------------------------------------------------
# Validation: every line judged, old manifest never clobbered
# ---------------------------------------------------------------------------

class SettleValidationTest(SettleCase):
    def test_every_line_is_validated_and_failures_keep_old_manifest(self):
        self.apply([put("A", "r1", 1, "E", 100, 0),
                    put("A", "r2", 1, "E", 200, 5)])
        out = os.path.join(self.dir, "out.jsonl")
        with open(out, "w", encoding="utf-8") as fh:
            fh.write("delivered\n")
        bad_lines = [
            '{"device": "B"',                          # broken JSON
            '[1, 2]',                                  # not an object
            '"just a string"',                         # not an object
            json.dumps({"job": "J", "start": 0, "end": 5}),     # no device
            json.dumps({"device": "B", "start": 0, "end": 5}),  # no job
            json.dumps({"device": "", "job": "J", "start": 0,
                        "end": 5}),                              # empty device
            json.dumps({"device": "B", "job": "", "start": 0,
                        "end": 5}),                              # empty job
            json.dumps({"device": "B", "job": 7, "start": 0, "end": 5}),
            json.dumps({"device": "B", "job": "J", "start": 5,
                        "end": 5}),                              # empty range
            json.dumps({"device": "B", "job": "J", "start": 6,
                        "end": 5}),                              # backwards
            json.dumps({"device": "B", "job": "J", "start": -1,
                        "end": 5}),                              # negative
            json.dumps({"device": "B", "job": "J", "start": 0,
                        "end": 2 ** 53}),                        # end too big
            json.dumps({"device": "B", "job": "J", "start": 0.5,
                        "end": 5}),                              # not an int
            json.dumps({"device": "B", "job": "J", "start": True,
                        "end": 5}),                              # bool
            '{"device": "B", "job": "J", "start": 0, "end": NaN}',
        ]
        for i, bad in enumerate(bad_lines):
            with self.subTest(line=i):
                # The bad line concerns device B, not the settled
                # device A: it must still fail the whole export.
                path = self.write_assignments(
                    [assignment("A", "J", 0, 1000), bad])
                result = self.settle(["A"], 0, 1000, path, output=out)
                self.assertEqual(result.returncode, 2)
                with open(out, encoding="utf-8") as fh:
                    self.assertEqual(fh.read(), "delivered\n")
        # A missing assignment file is an input failure too.
        result = self.settle(["A"], 0, 1000,
                             os.path.join(self.dir, "no-such.jsonl"),
                             output=out)
        self.assertEqual(result.returncode, 2)
        with open(out, encoding="utf-8") as fh:
            self.assertEqual(fh.read(), "delivered\n")

    def test_lines_for_other_devices_are_ignored_but_valid(self):
        self.apply([put("A", "r1", 1, "E", 100, 0),
                    put("A", "r2", 1, "E", 200, 5)])
        path = self.write_assignments([
            assignment("B", "JX", 0, 1000),     # not selected: validated
            assignment("A", "J", 0, 1000),
            "",                                 # blank lines carry nothing
            assignment("C", "JY", 20, 30),
            {"device": "A", "job": "J", "start": 0, "end": 1000,
             "note": "extra fields are fine"},  # concurrent same job
        ])
        got = self.settle_rows(["A"], 0, 1000, path)
        self.assertEqual(len(got), 1)
        self.assertEqual(got[0]["job"], "J")

    def test_output_file_atomicity_and_stdout_default(self):
        self.apply([put("A", "r1", 1, "E", 100, 0),
                    put("A", "r2", 1, "E", 200, 5)])
        path = self.write_assignments([assignment("A", "J", 0, 1000)])
        # Default output is stdout when -o is not given.
        result = run_tool(["settle", self.db, "--from", "0", "--to",
                           "1000", "--assign", path, "A"])
        self.assertEqual(result.returncode, 0, result.stderr)
        rows = [json.loads(x) for x in result.stdout.splitlines()]
        self.assertEqual(len(rows), 1)
        # A real file gets the same content, atomically replaced.
        out = os.path.join(self.dir, "out.jsonl")
        result = self.settle(["A"], 0, 1000, path, output=out)
        self.assertEqual(result.returncode, 0, result.stderr)
        with open(out, encoding="utf-8") as fh:
            self.assertEqual([json.loads(x) for x in fh], rows)
        # An unwritable destination reports an I/O failure (exit 1)
        # and creates nothing.
        missing = os.path.join(self.dir, "no-dir", "out.jsonl")
        result = self.settle(["A"], 0, 1000, path, output=missing)
        self.assertEqual(result.returncode, 1)
        self.assertFalse(os.path.exists(missing))

    def test_empty_export_still_delivers_a_manifest(self):
        path = self.write_assignments([assignment("A", "J", 0, 10)])
        out = os.path.join(self.dir, "out.jsonl")
        result = self.settle(["ghost"], 0, 1000, path, output=out)
        self.assertEqual(result.returncode, 0, result.stderr)
        with open(out, encoding="utf-8") as fh:
            self.assertEqual(fh.read(), "")

    def test_settle_does_not_write_back_to_the_ledger(self):
        self.apply([put("NL", "r1", 1, "E", 100, 0),
                    put("NL", "r2", 1, "E", 200, 5)])
        before = self.query("NL", 0, SHIFT)
        path = self.write_assignments([assignment("NL", "J", 0, 1000)])
        self.settle_rows(["NL"], 0, 1000, path)
        self.assertEqual(self.query("NL", 0, SHIFT), before)
        # A later apply still lands cleanly on the untouched ledger.
        self.apply([put("NL", "r3", 1, "E", 300, 9)])
        self.check("NL", 0, SHIFT)


# ---------------------------------------------------------------------------
# Density and nesting: no per-second expansion, no cross product
# ---------------------------------------------------------------------------

class SettleScaleTest(SettleCase):
    def test_nested_assignments_dense_readings(self):
        n_readings = 3000
        n_rows = 1200
        base = 1000
        for chunk in range(3):
            lines = []
            for i in range(chunk * n_readings // 3,
                           (chunk + 1) * n_readings // 3):
                lines.append(put("F", f"r{i}", 1, "E", base + i, 100 + i))
            self.apply(lines)
        # Two jobs, each nested n_rows deep across the whole window:
        # every second has both jobs, so every interval is one overlap
        # line — never readings x assignments detail rows.
        rows = []
        for i in range(n_rows):
            rows.append(assignment("F", "J1", i,
                                   base + n_readings + 1000 - i))
            rows.append(assignment("F", "J2", i + 1,
                                   base + n_readings + 999 - i))
        path = self.write_assignments(rows)
        started = time.monotonic()
        got = self.settle_rows(["F"], base, base + n_readings, path)
        elapsed = time.monotonic() - started
        self.assertEqual(len(got), n_readings - 1)
        for r in got:
            self.assertEqual(r["status"], "overlap")
            self.assertIsNone(r["job"])
            self.assertEqual(r["amount"], 1)
        self.assertEqual(got[0]["start"], base)
        self.assertEqual(got[-1]["end"], base + n_readings - 1)
        for a, b in zip(got, got[1:]):
            self.assertEqual(a["end"], b["start"])
        self.assertLess(elapsed, 30,
                        "nested assignments must not explode per second "
                        "or per interval x row")

    def test_window_skips_outlying_history(self):
        # A long history before the window must not be walked: only the
        # one predecessor of the window is fetched.
        self.apply([put("G", f"old{i}", 1, "E", i, i)
                    for i in range(2000)])
        self.apply([put("G", "w0", 1, "E", 5000, 3000),
                    put("G", "w1", 1, "E", 5100, 3010),
                    put("G", "w2", 1, "E", 5200, 3025)])
        path = self.write_assignments([assignment("G", "J", 0, 10000)])
        started = time.monotonic()
        got = self.settle_rows(["G"], 5000, 5300, path)
        elapsed = time.monotonic() - started
        # The interval ending at 5000 belongs to this window; its left
        # end is the true predecessor at 1999, not the range start.
        self.assertEqual([(r["start"], r["end"], r["amount"])
                          for r in got],
                         [(1999, 5000, 1001), (5000, 5100, 10),
                          (5100, 5200, 15)])
        self.assertLess(elapsed, 10)
        # [2000, 5000) contains no right end: the long old history and
        # the interval [1999, 5000) contribute nothing to it.
        self.assertEqual(self.settle_rows(["G"], 2000, 5000, path), [])


# ---------------------------------------------------------------------------
# One committed snapshot, even under a concurrent apply
# ---------------------------------------------------------------------------

class SettleConcurrencyTest(SettleCase):
    def test_snapshot_is_consistent_during_concurrent_apply(self):
        self.apply([put("G", "r1", 1, "E", 100, 0),
                    put("G", "r2", 1, "E", 200, 10),
                    put("G", "r3", 1, "E", 300, 20)])
        fifo = os.path.join(self.dir, "assign.fifo")
        os.mkfifo(fifo)
        out = os.path.join(self.dir, "out.jsonl")
        proc = subprocess.Popen(
            [sys.executable, "-I", TOOL, "settle", self.db,
             "--from", "0", "--to", "400", "--assign", fifo,
             "-o", out, "G"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        # The tool pins its ledger snapshot BEFORE opening the
        # assignment file; this open pairs with its reader only after
        # that, so the apply below commits strictly after the snapshot.
        with open(fifo, "w", encoding="utf-8") as fh:
            self.apply([put("G", "r4", 1, "E", 250, 15),
                        put("G", "r2", 2, "E", 200, 12)])
            fh.write(json.dumps(assignment("G", "J", 0, 1000)) + "\n")
        stdout, stderr = proc.communicate()
        self.assertEqual(proc.returncode, 0, stderr)
        with open(out, encoding="utf-8") as fh:
            got = [json.loads(x) for x in fh if x.strip()]
        # The manifest reflects only the pre-apply state: r4 does not
        # exist and r2 is still revision 1 with counter 10 — no splice
        # of the two ledger versions.
        self.assertEqual(got, [
            {"device": "G", "start": 100, "end": 200, "left_id": "r1",
             "left_revision": 1, "right_id": "r2", "right_revision": 1,
             "status": "allocated", "job": "J", "amount": 10},
            {"device": "G", "start": 200, "end": 300, "left_id": "r2",
             "left_revision": 1, "right_id": "r3", "right_revision": 1,
             "status": "allocated", "job": "J", "amount": 10}])
        # The ledger itself did move on.
        self.assertEqual(effective_points(self.accepted, "G"),
                         [(100, "E", 0), (200, "E", 12), (250, "E", 15),
                          (300, "E", 20)])


# ---------------------------------------------------------------------------
# Randomized differential test against the oracle
# ---------------------------------------------------------------------------

class SettleRandomizedTest(SettleCase):
    def test_random_scenarios(self):
        rng = random.Random(20261123)
        devices = ["d1", "d2"]
        jobs = ["J1", "J2", "J3"]
        live = {}    # (device, rid) -> highest revision applied
        points = {}  # (device, rid) -> second of the effective put
        used = {d: set() for d in devices}
        for trial in range(25):
            with self.subTest(trial=trial):
                for _ in range(rng.randint(1, 3)):
                    batch = []
                    batch_ids = set()
                    for _ in range(rng.randint(1, 6)):
                        dev = rng.choice(devices)
                        rid = f"r{rng.randint(0, 7)}"
                        key = (dev, rid)
                        if key in batch_ids:
                            continue
                        rev = live.get(key, 0) + rng.choice([1, 1, 2])
                        if key in points and rng.random() < 0.15:
                            batch.append(delete(dev, rid, rev))
                            batch_ids.add(key)
                            live[key] = rev
                            used[dev].discard(points.pop(key))
                            continue
                        old_t = points.get(key)
                        free = [x for x in range(601)
                                if x not in used[dev] or x == old_t]
                        if not free:
                            continue
                        t = rng.choice(free)
                        batch.append(put(dev, rid, rev,
                                         rng.choice(["E1", "E1", "E2"]),
                                         t, rng.randint(0, 50)))
                        batch_ids.add(key)
                        live[key] = rev
                        if old_t is not None:
                            used[dev].discard(old_t)
                        points[key] = t
                        used[dev].add(t)
                    if batch:
                        self.apply(batch)
                assigns = []
                for _ in range(rng.randint(0, 8)):
                    s = rng.randint(0, 600)
                    assigns.append(assignment(
                        rng.choice(devices + ["other"]),
                        rng.choice(jobs), s, s + rng.randint(1, 200)))
                if assigns and rng.random() < 0.5:
                    assigns.append(dict(assigns[0]))  # exact duplicate
                t_from = rng.randint(0, 500)
                t_to = t_from + rng.randint(1, 250)
                self.check_settle(devices, t_from, t_to, assigns)


if __name__ == "__main__":
    unittest.main()
