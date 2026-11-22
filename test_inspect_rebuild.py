#!/usr/bin/env python3
"""Tests for inspect_rebuild.py.

The oracle below is an independent, deliberately naive byte-by-byte model of
the specification: it materialises one coverage set per logical byte and
decides every record by looking at bytes, never using the tool's code.

Run:  python3 -I test_inspect_rebuild.py
"""

import hashlib
import json
import os
import random
import resource
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
import zlib

HERE = os.path.dirname(os.path.abspath(__file__))
TOOL = os.path.join(HERE, "inspect_rebuild.py")
MAX_LEN = 65536


# ---------------------------------------------------------------------------
# Fixture construction

def payload_of(obj, ensure_ascii=False):
    return json.dumps(obj, ensure_ascii=ensure_ascii,
                      separators=(",", ":")).encode("utf-8")


def make_record(obj):
    payload = payload_of(obj)
    return (len(payload).to_bytes(4, "little") + payload
            + (zlib.crc32(payload) & 0xFFFFFFFF).to_bytes(4, "little"))


def make_raw_record(payload, length_override=None, crc_override=None):
    length = len(payload) if length_override is None else length_override
    crc = (zlib.crc32(payload) & 0xFFFFFFFF) if crc_override is None else crc_override
    return length.to_bytes(4, "little") + payload + crc.to_bytes(4, "little")


class Fixture:
    """A temporary directory holding a catalog and its fragment files."""

    def __init__(self, total_size, sync_points, logical_writes, fragments,
                 *, relative_paths=True, catalog_name="catalog.json"):
        self.dir = tempfile.mkdtemp(prefix="inspect-fixture-")
        self.total_size = total_size
        self.sync_points = list(sync_points)
        self.entries = []
        for index, (start, data, filename) in enumerate(fragments):
            path = os.path.join(self.dir, filename)
            with open(path, "wb") as fh:
                fh.write(data)
            shown = filename if relative_paths else path
            self.entries.append({"path": shown, "start": start,
                                 "length": len(data)})
        self.catalog_path = os.path.join(self.dir, catalog_name)
        self.write_catalog()

    def write_catalog(self, **overrides):
        catalog = {"total_size": self.total_size,
                   "sync_points": self.sync_points,
                   "fragments": self.entries}
        catalog.update(overrides)
        with open(self.catalog_path, "w", encoding="utf-8") as fh:
            json.dump(catalog, fh)

    def cleanup(self):
        shutil.rmtree(self.dir, ignore_errors=True)


def base_dir():
    return tempfile.mkdtemp(prefix="inspect-base-")


def write_fragment(directory, filename, start, data):
    """A fragment file holds only its own bytes; `start` is logical."""
    path = os.path.join(directory, filename)
    with open(path, "wb") as fh:
        fh.write(data)
    return {"path": filename, "start": start, "length": len(data)}, path


def write_catalog(directory, total_size, sync_points, entries, name="catalog.json"):
    path = os.path.join(directory, name)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump({"total_size": total_size, "sync_points": sync_points,
                   "fragments": entries}, fh)
    return path


# ---------------------------------------------------------------------------
# Independent oracle

def oracle_lines(fragments, total_size, sync_points):
    """fragments: [(start, bytes)].  Returns [(type, start, end, obj_or_None)].

    Positions are examined one by one.  A position is a conflict when two
    copies disagree; it is a hole when no copy covers it.
    """
    coverage = {}
    for start, data in fragments:
        for offset, value in enumerate(data):
            position = start + offset
            if 0 <= position < total_size:
                coverage.setdefault(position, set()).add(value)

    def classify(a, b):
        values = []
        conflict = False
        gap = False
        for p in range(a, b):
            seen = coverage.get(p)
            if not seen:
                gap = True
                continue
            if len(seen) > 1:
                conflict = True
            else:
                values.append(next(iter(seen)))
        if conflict:
            return "conflict", None
        if gap:
            return "gap", None
        return "complete", bytes(values)

    def valid(record):
        payload = record[4:-4]
        stored = int.from_bytes(record[-4:], "little")
        if (zlib.crc32(payload) & 0xFFFFFFFF) != stored:
            return None
        try:
            text = payload.decode("utf-8")
            obj = json.loads(text)
        except (UnicodeDecodeError, ValueError):
            return None
        return obj if isinstance(obj, dict) else None

    lines = []
    bounds = list(sync_points) + [total_size]
    if total_size == 0:
        return lines
    for i in range(len(bounds) - 1):
        seg_start, seg_end = bounds[i], bounds[i + 1]
        pos = seg_start
        while pos < seg_end:
            if pos + 4 > seg_end:
                lines.append(("boundary_unknown", pos, seg_end, None))
                break
            status, head = classify(pos, pos + 4)
            if status != "complete":
                lines.append(("boundary_unknown", pos, seg_end, None))
                break
            length = int.from_bytes(head, "little")
            if not (1 <= length <= MAX_LEN):
                lines.append(("boundary_unknown", pos, seg_end, None))
                break
            end = pos + 8 + length
            if end > seg_end:
                lines.append(("boundary_unknown", pos, seg_end, None))
                break
            status, record = classify(pos, end)
            if status == "conflict":
                lines.append(("conflict", pos, end, None))
            elif status == "gap":
                lines.append(("missing", pos, end, None))
            else:
                obj = valid(record)
                if obj is None:
                    lines.append(("invalid", pos, end, None))
                else:
                    lines.append(("record", pos, end, obj))
            pos = end
    return lines


# ---------------------------------------------------------------------------
# Running the tool

def run_tool(catalog_path, output_path=None, cwd=None):
    command = [sys.executable, "-I", TOOL, catalog_path]
    if output_path is not None:
        command += ["-o", output_path]
    return subprocess.run(command, capture_output=True, text=True, cwd=cwd)


def parse_output(text):
    lines = []
    for line in text.splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        if set(obj) - {"type", "start", "end", "object"}:
            raise AssertionError(f"unexpected keys in output line: {line}")
        lines.append((obj["type"], obj["start"], obj["end"],
                      obj.get("object")))
    return lines


def check_tiling(testcase, lines, total_size):
    if total_size == 0:
        testcase.assertEqual(lines, [])
        return
    testcase.assertEqual(lines[0][1], 0, "first line must start at 0")
    testcase.assertEqual(lines[-1][2], total_size,
                         "last line must end at total_size")
    for previous, current in zip(lines, lines[1:]):
        testcase.assertEqual(previous[2], current[1],
                             "lines must be contiguous")
    for kind, start, end, _ in lines:
        testcase.assertLess(start, end)


def read_file(path):
    with open(path, "rb") as fh:
        return fh.read()


# ---------------------------------------------------------------------------
# Manual fixture A: every interesting damage in one stream

class ManualStream:
    """Byte layout documented position by position, then cut into copies."""

    def __init__(self):
        self.r = {}
        self.logical = bytearray()
        self.bounds = {}
        self.objects = {}
        names = ["R1", "R2", "R3", "R4", "R5", "Rcrc", "R6", "AREA", "R8", "R9"]
        payload_objects = {
            "R1": {"id": 1, "text": "héllo"},
            "R2": {"id": 2, "values": list(range(20))},
            "R3": {"id": 3},
            "R4": {"id": 4, "pad": "x" * 40},
            "R5": {"id": 5},
            "R6": {"id": 6},
            "R8": {"id": 8, "text": "ok"},
            "R9": {"id": 9, "tail": True},
        }
        records = {name: make_record(obj) for name, obj in payload_objects.items()}
        for name, obj in payload_objects.items():
            self.objects[name] = obj
        # A record whose stored CRC no longer matches its payload (payload
        # byte corrupted while the original CRC was kept).
        crc_bad = make_record({"id": "crcbad", "pad": "y" * 10})
        crc_bad = crc_bad[:4] + bytes([crc_bad[4] ^ 0xFF]) + crc_bad[5:]
        records["Rcrc"] = bytes(crc_bad)
        # The untrustworthy-length area: four zero length bytes followed by a
        # byte sequence that itself parses as a perfectly valid record.
        decoy = make_record({"id": "decoy", "should": "not be resynced"})
        area = b"\x00\x00\x00\x00" + decoy + b"\x00\x00\x00\x00"
        records["AREA"] = area

        for name in names:
            self.bounds[name] = (len(self.logical),
                                 len(self.logical) + len(records[name]))
            self.logical += records[name]
        self.total = len(self.logical)
        self.sync_points = [self.bounds["R1"][0], self.bounds["R6"][0],
                            self.bounds["R8"][0]]
        # Sanity: sync points really are record starts.
        assert self.sync_points == [0, self.bounds["R6"][0], self.bounds["R8"][0]]

    def build_fragments(self):
        """(start, bytes, filename) copies, with deliberate holes/splits."""
        L = bytes(self.logical)
        b = self.bounds
        s3 = b["R4"][0]
        flip = s3 + 16
        copies = [
            # The 4-byte length of R1 is split over two fragments.
            (0, L[0:2], "f0a.bin"),
            (2, L[2:b["R2"][1] - 2], "f0b.bin"),
            # Consistent overlap with the previous fragment across the split
            # inside R2's payload.
            (b["R2"][1] - 2, L[b["R2"][1] - 2:b["R3"][1]], "f1.bin"),
            # R4 head (length + start of payload) ...
            (s3, L[s3:s3 + 20], "f2.bin"),
            # ... a shifted copy whose byte at s3+16 disagrees with f2 ...
            (s3 + 15, L[s3 + 15:s3 + 25], "f2b.bin"),
            # ... a hole [s3+25, s3+30), then the rest of R4, plus R5 and two
            # bytes of Rcrc.
            (s3 + 30, L[s3 + 30:b["Rcrc"][0] + 2], "f3.bin"),
        ]
        mutated = bytearray(copies[4][1])
        mutated[flip - (s3 + 15)] ^= 0xFF
        copies[4] = (s3 + 15, bytes(mutated), "f2b.bin")
        copies += [
            # Identical overlap over Rcrc's first two bytes, then R6 and the
            # untrustworthy-length area.
            (b["Rcrc"][0], L[b["Rcrc"][0]:b["AREA"][1]], "f4.bin"),
            # Last segment.
            (b["R8"][0], L[b["R8"][0]:self.total], "f5.bin"),
            # Exact duplicate of R1's bytes: must not change anything.
            (0, L[0:b["R1"][1]], "dup.bin"),
        ]
        return copies

    def expected(self):
        b = self.bounds
        return [
            ("record", b["R1"][0], b["R1"][1], self.objects["R1"]),
            ("record", b["R2"][0], b["R2"][1], self.objects["R2"]),
            ("record", b["R3"][0], b["R3"][1], self.objects["R3"]),
            ("conflict", b["R4"][0], b["R4"][1], None),
            ("record", b["R5"][0], b["R5"][1], self.objects["R5"]),
            ("invalid", b["Rcrc"][0], b["Rcrc"][1], None),
            ("record", b["R6"][0], b["R6"][1], self.objects["R6"]),
            ("boundary_unknown", b["AREA"][0], b["AREA"][1], None),
            ("record", b["R8"][0], b["R8"][1], self.objects["R8"]),
            ("record", b["R9"][0], b["R9"][1], self.objects["R9"]),
        ]


class ManualFixtureTests(unittest.TestCase):

    def setUp(self):
        self.stream = ManualStream()
        self.copies = self.stream.build_fragments()
        self.fixture = Fixture(self.stream.total, self.stream.sync_points, None,
                               self.copies)

    def tearDown(self):
        self.fixture.cleanup()

    def test_manual_expected_lines(self):
        result = run_tool(self.fixture.catalog_path)
        self.assertEqual(result.returncode, 0, result.stderr)
        lines = parse_output(result.stdout)
        self.assertEqual(lines, self.stream.expected())
        check_tiling(self, lines, self.stream.total)
        self.assertEqual(lines, oracle_lines(
            [(s, d) for s, d, _ in self.copies],
            self.stream.total, self.stream.sync_points))

    def test_byte_level_claims(self):
        """For each segment, assert the delivered classification per byte."""
        result = run_tool(self.fixture.catalog_path)
        lines = parse_output(result.stdout)
        coverage = {}
        for start, data, _ in self.copies:
            for offset, value in enumerate(data):
                coverage.setdefault(start + offset, []).append(value)
        copies = {p: v for p, v in coverage.items()}
        for kind, start, end, _ in lines:
            if kind in ("record", "invalid", "missing", "conflict"):
                # A record line must not cross a trusted sync point.
                for sync in self.stream.sync_points[1:]:
                    self.assertFalse(start < sync < end)
                conflicted = any(len(set(copies.get(p, ()))) > 1
                                 for p in range(start, end))
                holed = any(p not in copies for p in range(start, end))
                if kind == "conflict":
                    self.assertTrue(conflicted, (kind, start, end))
                if kind == "missing":
                    self.assertFalse(conflicted)
                    self.assertTrue(holed)
                if kind == "record":
                    self.assertFalse(conflicted or holed)
                    n = int.from_bytes(
                        bytes(copies[p][0] for p in range(start, start + 4)),
                        "little")
                    self.assertEqual(end - start, 8 + n)
                if kind == "invalid":
                    self.assertFalse(conflicted or holed)

    def test_order_independence(self):
        baseline = parse_output(run_tool(self.fixture.catalog_path).stdout)
        rng = random.Random(12345)
        for _ in range(8):
            rng.shuffle(self.fixture.entries)
            self.fixture.write_catalog()
            result = run_tool(self.fixture.catalog_path)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(parse_output(result.stdout), baseline)

    def test_duplicates_change_nothing(self):
        baseline = parse_output(run_tool(self.fixture.catalog_path).stdout)
        for entry in list(self.fixture.entries):
            self.fixture.entries.append(dict(entry))
        self.fixture.write_catalog()
        result = run_tool(self.fixture.catalog_path)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(parse_output(result.stdout), baseline)

    def test_relative_and_absolute_paths_same_result(self):
        baseline = parse_output(run_tool(self.fixture.catalog_path).stdout)
        absolute = Fixture(self.stream.total, self.stream.sync_points, None,
                           self.copies, relative_paths=False)
        try:
            result = run_tool(absolute.catalog_path,
                              cwd=base_dir())  # run from elsewhere
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(parse_output(result.stdout), baseline)
        finally:
            absolute.cleanup()

    def test_fragments_unmodified(self):
        before = {}
        for entry in self.fixture.entries:
            path = os.path.join(self.fixture.dir, entry["path"])
            before[path] = hashlib.sha256(read_file(path)).hexdigest()
        self.assertEqual(run_tool(self.fixture.catalog_path).returncode, 0)
        for path, digest in before.items():
            self.assertEqual(hashlib.sha256(read_file(path)).hexdigest(), digest)


# ---------------------------------------------------------------------------
# Focused boundary tests

def run_simple(testcase, total, syncs, copies, expected):
    fixture = Fixture(total, syncs, None, copies)
    try:
        result = run_tool(fixture.catalog_path)
        testcase.assertEqual(result.returncode, 0, result.stderr)
        lines = parse_output(result.stdout)
        testcase.assertEqual(lines, expected)
        check_tiling(testcase, lines, total)
        testcase.assertEqual(
            lines, oracle_lines([(s, d) for s, d, _ in copies], total, syncs))
        return lines
    finally:
        fixture.cleanup()


class BoundaryTests(unittest.TestCase):

    def test_invalid_length_values(self):
        good = make_record({"ok": 1})
        for bad_length in (0, MAX_LEN + 1, 0xFFFFFFFF):
            with self.subTest(bad_length=bad_length):
                bad = bad_length.to_bytes(4, "little") + b"junkjunk"
                next_sync = len(bad)
                total = next_sync + len(good)
                logical = bad + good
                copies = [(0, logical, "a.bin"), (next_sync, good, "b.bin")]
                run_simple(self, total, [0, next_sync], copies, [
                    ("boundary_unknown", 0, next_sync, None),
                    ("record", next_sync, total, {"ok": 1}),
                ])

    def test_length_crosses_sync(self):
        first = make_record({"a": 1})
        second = make_record({"b": 2})
        # First record's length claims a record that would swallow the sync
        # point at the start of the second record.
        lying = first[:4] + first[4:]  # intact
        overrun_end = len(first) + len(second)
        sync = len(first)
        # Make the length at 0 so large that end > sync.
        overrun = (len(first)).to_bytes(4, "little")
        data = overrun + first[4:] + second
        total = sync + len(second)
        copies = [(0, data, "a.bin"), (sync, second, "b.bin")]
        run_simple(self, total, [0, sync], copies, [
            ("boundary_unknown", 0, sync, None),
            ("record", sync, total, {"b": 2}),
        ])

    def test_length_field_gap_and_conflict(self):
        rec = make_record({"x": 1})
        total = len(rec)
        # Length split with a hole in the middle.
        run_simple(self, total, [0], [
            (0, rec[0:1], "a.bin"), (2, rec[2:], "b.bin")],
            [("boundary_unknown", 0, total, None)])
        # Two copies of the length disagree; one of them is right.
        other = bytearray(rec)
        other[0] ^= 1
        run_simple(self, total, [0], [
            (0, rec, "a.bin"), (0, bytes(other), "b.bin")],
            [("boundary_unknown", 0, total, None)])

    def test_length_field_straddles_sync(self):
        # Only 3 bytes before the next trusted sync point.
        data = b"\x05\x00\x00XYYYYYYY"
        total = len(data)
        run_simple(self, total, [0, 3], [
            (0, data, "a.bin")],
            [("boundary_unknown", 0, 3, None),
             ("boundary_unknown", 3, total, None)])

    def test_crc_cannot_resolve_conflict(self):
        rec = bytearray(make_record({"v": "original"}))
        changed = bytearray(rec)
        changed[-5] ^= 0xFF
        # Recompute the CRC on the altered copy: it is self-consistent but
        # disagrees with the other copy, so it is still a conflict.
        changed[-4:] = (zlib.crc32(bytes(changed[4:-4])) & 0xFFFFFFFF).to_bytes(4, "little")
        total = len(rec)
        run_simple(self, total, [0], [
            (0, bytes(rec), "a.bin"), (0, bytes(changed), "b.bin")],
            [("conflict", 0, total, None)])

    def test_missing_does_not_invent_zero_bytes(self):
        rec = make_record({"m": 1})
        total = len(rec)
        # A one-byte hole in the payload: never filled with 0x00.
        copies = [(0, rec[:8], "a.bin"), (9, rec[9:], "b.bin")]
        lines = run_simple(self, total, [0], copies,
                           [("missing", 0, total, None)])
        self.assertEqual(lines[0][0], "missing")

    def test_invalid_variants(self):
        cases = {
            "crc": make_raw_record(payload_of({"id": 1}),
                                   crc_override=0xDEADBEEF),
            "utf8": make_raw_record(b'{"id":1,"x":"\xff\xfe"}'),
            "json": make_raw_record(b'{not json at all}'),
            "non_object": make_raw_record(b"[1,2,3]"),
        }
        for name, rec in cases.items():
            with self.subTest(case=name):
                run_simple(self, len(rec), [0], [(0, rec, "a.bin")],
                           [("invalid", 0, len(rec), None)])

    def test_dict_object_only(self):
        rec = make_record({"nested": {"a": [1, 2, None, True]}, "s": "µ"})
        run_simple(self, len(rec), [0], [(0, rec, "a.bin")],
                   [("record", 0, len(rec), {"nested": {"a": [1, 2, None, True]},
                                             "s": "µ"})])

    def test_no_fragments(self):
        run_simple(self, 100, [0, 40], [],
                   [("boundary_unknown", 0, 40, None),
                    ("boundary_unknown", 40, 100, None)])

    def test_total_size_zero(self):
        fixture = Fixture(0, [], None, [])
        try:
            out = os.path.join(fixture.dir, "out.jsonl")
            result = run_tool(fixture.catalog_path, out)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(read_file(out), b"")
        finally:
            fixture.cleanup()

    def test_payload_length_limits(self):
        for length in (1, MAX_LEN):
            with self.subTest(length=length):
                payload = b"x" * length
                rec = make_raw_record(payload)
                run_simple(self, len(rec), [0], [(0, rec, "a.bin")],
                           [("invalid", 0, len(rec), None)])
                                # CRC is valid; payload is not JSON.

    def test_sync_segment_ends_exactly(self):
        # Several records, sync points only at 0: complete walk to the end.
        recs = [make_record({"i": i}) for i in range(5)]
        blob = b"".join(recs)
        expected, pos = [], 0
        for i, rec in enumerate(recs):
            expected.append(("record", pos, pos + len(rec), {"i": i}))
            pos += len(rec)
        run_simple(self, len(blob), [0], [(0, blob, "a.bin")], expected)

    def test_last_segment_short_garbage(self):
        rec = make_record({"i": 1})
        blob = rec + b"\x01\x02"
        run_simple(self, len(blob), [0], [(0, blob, "a.bin")], [
            ("record", 0, len(rec), {"i": 1}),
            ("boundary_unknown", len(rec), len(blob), None),
        ])


# ---------------------------------------------------------------------------
# Randomized oracle comparison

def random_case(rng):
    objects = []
    for _ in range(rng.randint(1, 12)):
        kind = rng.random()
        if kind < 0.5:
            obj = {"id": len(objects),
                   "text": "".join(rng.choice("abé中") for _ in range(rng.randint(0, 15))),
                   "n": rng.randint(-5, 10 ** 9)}
        elif kind < 0.7:
            obj = {"id": len(objects), "nested": {"a": [1, 2, {"b": "c"}]}}
        else:
            obj = {"id": len(objects)}
        objects.append(obj)
    records = [make_record(o) for o in objects]
    # Occasionally corrupt a whole record in the logical stream (CRC error,
    # bad UTF-8, non-object payload) but leave its length intact.
    for i in range(len(records)):
        if rng.random() < 0.15:
            raw = bytearray(records[i])
            choice = rng.randrange(3)
            if choice == 0:
                raw[-5] ^= 0x7F                      # CRC mismatch
            elif choice == 1:
                raw[5] ^= 0xFF                       # maybe UTF-8 garbage
            else:
                payload = b"[1]"
                raw = make_raw_record(payload)       # valid CRC, non-object
            records[i] = bytes(raw)
    starts = []
    pos = 0
    for rec in records:
        starts.append(pos)
        pos += len(rec)
    total = pos
    if total == 0:
        return None
    # Sync points: trusted record starts.  Nothing else may be used as one.
    candidates = starts[1:]
    syncs = [0]
    for cand in candidates:
        if rng.random() < 0.35:
            syncs.append(cand)
    syncs = sorted(set(syncs))

    blob = bytearray(b"".join(records))
    copies = []
    if rng.random() < 0.1:
        copies = []                       # no fragments at all
    else:
        for k in range(rng.randint(1, 12)):
            a = rng.randrange(total)
            span = rng.choice([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15, 20, 50,
                               200, 400])
            b = min(total, a + span)
            data = bytearray(blob[a:b])
            if rng.random() < 0.25 and len(data) > 1:
                # Mutate a few bytes; this makes positions conflict.
                for _ in range(rng.randint(1, 2)):
                    idx = rng.randrange(len(data))
                    data[idx] ^= rng.randint(1, 255)
            copies.append((a, bytes(data), f"c{k}.bin"))
        if rng.random() < 0.4:
            copies.append((copies[0][0], copies[0][1], "dup.bin"))
        # Adjacent, non-overlapping fragments abound in real data: make sure
        # they are represented.
        if rng.random() < 0.5 and total > 4:
            cuts = sorted({0, total} | {rng.randrange(1, total)
                                        for _ in range(rng.randint(1, 6))})
            for j in range(len(cuts) - 1):
                lo, hi = cuts[j], cuts[j + 1]
                if rng.random() < 0.7:
                    copies.append((lo, bytes(blob[lo:hi]), f"n{j}.bin"))
    return total, syncs, copies


class RandomOracleTests(unittest.TestCase):

    def test_non_adjacent_copies_do_not_fake_conflict(self):
        # Sorted by start: f0 decides [0,6), f1 covers [4,12) and straddles
        # f0's end, f2 covers [10, end) and straddles f1's end.  f1 is the
        # only source for [6,10), so comparing it against undecided bytes
        # would invent a conflict.  Shared byte ranges are [4,6) and [10,12),
        # both inside the payload.
        blob = make_record({"k": 1, "pad": "abcdefghijklmnop"})
        self.assertGreater(len(blob), 16)
        copies = [
            (0, blob[0:6], "a.bin"),
            (4, blob[4:12], "b.bin"),
            (10, blob[10:], "c.bin"),
        ]
        run_simple(self, len(blob), [0], copies,
                   [("record", 0, len(blob), {"k": 1, "pad": "abcdefghijklmnop"})])
        # Same geometry, but b disagrees at the genuinely shared [4,6).
        bad = bytearray(blob[4:12])
        bad[0] ^= 0xFF
        copies = [
            (0, blob[0:6], "a.bin"),
            (4, bytes(bad), "b.bin"),
            (10, blob[10:], "c.bin"),
        ]
        run_simple(self, len(blob), [0], copies,
                   [("conflict", 0, len(blob), None)])

    def test_random_against_oracle(self):
        rng = random.Random(987654321)
        for case_index in range(250):
            case = random_case(rng)
            if case is None:
                continue
            total, syncs, copies = case
            fixture = Fixture(total, syncs, None, copies)
            try:
                result = run_tool(fixture.catalog_path)
                self.assertEqual(result.returncode, 0,
                                 f"case {case_index}: {result.stderr}")
                lines = parse_output(result.stdout)
                expected = oracle_lines(
                    [(s, d) for s, d, _ in copies], total, syncs)
                self.assertEqual(lines, expected, f"case {case_index}")
                check_tiling(self, lines, total)
                # Order independence on a subset of the cases.
                if case_index % 17 == 0:
                    shuffled = list(fixture.entries)
                    rng.shuffle(shuffled)
                    fixture.entries = shuffled
                    fixture.write_catalog()
                    result2 = run_tool(fixture.catalog_path)
                    self.assertEqual(result2.returncode, 0, result2.stderr)
                    self.assertEqual(parse_output(result2.stdout), expected)
            finally:
                fixture.cleanup()


# ---------------------------------------------------------------------------
# Large sparse logical space

class SparseTests(unittest.TestCase):

    def test_huge_hole_with_far_sync_points(self):
        total = 4 * 1024 ** 3 + 12345
        rec_a = make_record({"id": "far-a", "payload": "z" * 100})
        rec_b = make_record({"id": "far-b"})
        rec_c = make_record({"id": "middle"})
        remaining = len(rec_a) + len(rec_b)
        sync2 = total - remaining          # record start
        sync1 = (3 * total) // 4           # also under total, > any stored byte
        # Place a record at sync1, then everything to sync2 is a hole.
        directory = base_dir()
        try:
            entries = []
            entry, path = write_fragment(
                directory, "middle.bin", sync1, rec_c)
            entries.append(entry)
            entry, path2 = write_fragment(
                directory, "tail.bin", sync2, rec_a + rec_b)
            entries.append(entry)
            catalog = write_catalog(directory, total,
                                    [0, sync1, sync2], entries)
            for p in (path, path2):
                st = os.stat(p)
                self.assertLess(st.st_size, 4096)
                self.assertLess(st.st_blocks * 512, 65536)
            out = os.path.join(directory, "out.jsonl")

            wrapper = (
                "import json,resource,subprocess,sys\n"
                "r=subprocess.run(sys.argv[1:],capture_output=True)\n"
                "print(json.dumps({'code':r.returncode,'err':r.stderr.decode(),"
                "'maxrss':resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss}))\n"
            )
            result = subprocess.run(
                [sys.executable, "-I", "-c", wrapper,
                 sys.executable, "-I", TOOL, catalog, "-o", out],
                capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            info = json.loads(result.stdout)
            self.assertEqual(info["code"], 0, info["err"])
            self.assertLess(info["maxrss"], 200 * 1024,
                            f"max RSS {info['maxrss']} KiB is too high")

            lines = parse_output(read_file(out).decode())
            expected = [
                ("boundary_unknown", 0, sync1, None),
                ("record", sync1, sync1 + len(rec_c), {"id": "middle"}),
                ("boundary_unknown", sync1 + len(rec_c), sync2, None),
                ("record", sync2, sync2 + len(rec_a),
                 {"id": "far-a", "payload": "z" * 100}),
                ("record", sync2 + len(rec_a), total, {"id": "far-b"}),
            ]
            self.assertEqual(lines, expected)
            check_tiling(self, lines, total)
            self.assertEqual(lines, oracle_lines(
                [(sync1, rec_c), (sync2, rec_a + rec_b)], total,
                [0, sync1, sync2]))
            # The 3 GiB hole is not materialised in the output.
            self.assertLess(os.path.getsize(out), 4096)
        finally:
            shutil.rmtree(directory, ignore_errors=True)


# ---------------------------------------------------------------------------
# Failure behaviour and preservation of an existing deliverable

class FailureTests(unittest.TestCase):

    def setUp(self):
        self.directory = tempfile.mkdtemp(prefix="inspect-fail-")
        self.rec = make_record({"id": 1})
        self.entry, self.frag_path = write_fragment(
            self.directory, "a.bin", 0, self.rec)
        self.catalog = write_catalog(
            self.directory, len(self.rec), [0], [self.entry])
        self.out = os.path.join(self.directory, "out.jsonl")
        with open(self.out, "wb") as fh:
            fh.write(b'{"type":"record","start":0,"end":1,"object":{}}\n')

    def tearDown(self):
        for root, dirs, files in os.walk(self.directory):
            for name in dirs:
                try:
                    os.chmod(os.path.join(root, name), 0o755)
                except OSError:
                    pass
        shutil.rmtree(self.directory, ignore_errors=True)

    def entries(self):
        with open(self.catalog, "rb") as fh:
            return json.load(fh)["fragments"]

    def assert_fails_preserving(self, expected_code, catalog=None):
        old = read_file(self.out)
        result = run_tool(catalog or self.catalog, self.out)
        self.assertEqual(result.returncode, expected_code,
                         f"stderr: {result.stderr}")
        self.assertEqual(read_file(self.out), old,
                         "existing deliverable was modified")

    def rewrite_catalog(self, **changes):
        with open(self.catalog, "rb") as fh:
            data = json.load(fh)
        data.update(changes)
        with open(self.catalog, "w", encoding="utf-8") as fh:
            json.dump(data, fh)

    def test_declared_length_mismatch(self):
        self.rewrite_catalog(fragments=[
            {"path": "a.bin", "start": 0, "length": len(self.rec) + 1}])
        self.assert_fails_preserving(2)

    def test_fragment_missing(self):
        self.rewrite_catalog(fragments=[
            {"path": "nope.bin", "start": 0, "length": 4}])
        self.assert_fails_preserving(2)

    def test_fragment_is_directory(self):
        os.mkdir(os.path.join(self.directory, "dir.bin"))
        self.rewrite_catalog(fragments=[
            {"path": "dir.bin", "start": 0, "length": 4}])
        self.assert_fails_preserving(2)

    def test_fragment_out_of_range(self):
        self.rewrite_catalog(fragments=[
            {"path": "a.bin", "start": 1, "length": len(self.rec)}])
        self.assert_fails_preserving(2)

    def test_zero_length_fragment(self):
        self.rewrite_catalog(fragments=[
            {"path": "a.bin", "start": 0, "length": 0}])
        self.assert_fails_preserving(2)

    def test_catalog_not_json(self):
        with open(self.catalog, "w", encoding="utf-8") as fh:
            fh.write("{not json")
        self.assert_fails_preserving(2)

    def test_catalog_not_object(self):
        with open(self.catalog, "w", encoding="utf-8") as fh:
            fh.write("[1, 2, 3]")
        self.assert_fails_preserving(2)

    def test_missing_keys(self):
        for key in ("total_size", "sync_points", "fragments"):
            with self.subTest(missing=key):
                with open(self.catalog, "rb") as fh:
                    data = json.load(fh)
                saved = data.pop(key)
                with open(self.catalog, "w", encoding="utf-8") as fh:
                    json.dump(data, fh)
                self.assert_fails_preserving(2)
                data[key] = saved
                with open(self.catalog, "w", encoding="utf-8") as fh:
                    json.dump(data, fh)

    def test_bad_sync_points(self):
        cases = [
            [1],                        # first is not 0
            [0, 0],                     # not strictly increasing
            [0, 3, 2],                  # not increasing
            [0, len(self.rec)],         # not below total_size
            [0, -1],                    # negative
        ]
        for syncs in cases:
            with self.subTest(syncs=syncs):
                self.rewrite_catalog(sync_points=syncs)
                self.assert_fails_preserving(2)

    def test_bad_ints(self):
        cases = [
            {"total_size": -1},
            {"total_size": 1.5},
            {"total_size": True},
            {"total_size": "8"},
            {"total_size": len(self.rec),
             "fragments": [{"path": "a.bin", "start": True,
                            "length": 4}]},
            {"total_size": len(self.rec),
             "fragments": [{"path": "a.bin", "start": 0,
                            "length": "4"}]},
            {"total_size": len(self.rec),
             "fragments": [{"path": "", "start": 0, "length": 4}]},
            {"total_size": len(self.rec),
             "fragments": [{"path": "a.bin", "start": 0}]},
            {"total_size": 0, "sync_points": [0], "fragments": []},
            {"total_size": len(self.rec), "sync_points": [],
             "fragments": []},
        ]
        for changes in cases:
            with self.subTest(changes=changes):
                self.rewrite_catalog(**changes)
                self.assert_fails_preserving(2)

    def test_output_error_preserves_target(self):
        if os.geteuid() == 0:
            self.skipTest("permission bits are not enforced for root")
        os.chmod(self.directory, 0o555)
        try:
            self.assert_fails_preserving(1)
        finally:
            os.chmod(self.directory, 0o755)

    def test_output_in_missing_directory(self):
        target = os.path.join(self.directory, "no-such-dir", "out.jsonl")
        result = run_tool(self.catalog, target)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertFalse(os.path.exists(target))

    def test_output_is_directory(self):
        target = os.path.join(self.directory, "outdir")
        os.mkdir(target)
        result = run_tool(self.catalog, target)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertTrue(os.path.isdir(target))

    def test_runtime_read_failure_preserves_target(self):
        if os.geteuid() == 0:
            self.skipTest("permission bits are not enforced for root")
        # Validation (stat) passes, but opening the fragment at reconstruction
        # time fails: the deliverable must survive the late failure.
        os.chmod(self.frag_path, 0)
        try:
            self.assert_fails_preserving(2)
            leftovers = [n for n in os.listdir(self.directory) if ".tmp" in n]
            self.assertEqual(leftovers, [])
        finally:
            os.chmod(self.frag_path, 0o644)

    def test_stdout_mode(self):
        result = run_tool(self.catalog)          # no -o
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(parse_output(result.stdout),
                         [("record", 0, len(self.rec), {"id": 1})])
        self.assertEqual(result.stderr, "")

    def test_success_still_replaces_target(self):
        result = run_tool(self.catalog, self.out)
        self.assertEqual(result.returncode, 0, result.stderr)
        lines = parse_output(read_file(self.out).decode())
        self.assertEqual(lines, [("record", 0, len(self.rec), {"id": 1})])

    def test_no_temp_left_behind_on_failure(self):
        self.rewrite_catalog(fragments=[
            {"path": "a.bin", "start": 0, "length": len(self.rec) + 1}])
        self.assert_fails_preserving(2)
        leftovers = [n for n in os.listdir(self.directory) if ".tmp" in n]
        self.assertEqual(leftovers, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
