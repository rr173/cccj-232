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


if __name__ == "__main__":
    unittest.main()
