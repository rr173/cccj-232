#!/usr/bin/env python3
"""Rebuild a contiguous JSONL view of a binary inspection-record stream.

The stream is a logical byte space tiled into record units:

    4-byte unsigned little-endian payload length L   (1 <= L <= 65536)
    L bytes of UTF-8 JSON object text
    4-byte unsigned little-endian CRC32 of the payload

Only byte fragments of that logical space survive, delivered by a JSON
catalog.  This tool merges the fragments by *logical position* (never by
fragment order), reconstructs whole records from trusted sync points, and
emits one JSONL line per logical interval.  The lines tile the logical range
contiguously: the first line starts at 0, the end of every line equals the
start of the next, and the last line ends at the catalog total size.

A record is emitted as exactly one line, chosen in this order:

  record            all bytes present, no disagreement, CRC / UTF-8 / JSON
                     object all valid
  invalid            all bytes present, no disagreement, but the record fails
                     its CRC, its UTF-8 decoding, or does not hold a JSON object
  conflict           some byte position is covered twice with different values
  missing            no disagreement, but at least one byte position uncovered
  boundary_unknown   from the record start to the end of the sync segment;
                     the next record boundary cannot be known (bad / missing /
                     conflicting length field, or a length that overruns the
                     segment).  Processing resumes at the next trusted sync point.

Sync points are trusted record starts kept outside the stream.  Between two
sync points, and between the last sync point and the logical end, the bytes
form a whole number of records; no record crosses a sync point.  Those bounds
are therefore always tried first, so an untrustworthy length field never
shifts or loses the next trusted boundary.

Notes on semantics that are easy to get wrong:

* A value found in only one place is *usable* even if it happens to look like
  a zero fill; only "read twice with different values" is a conflict.  Sync
  points are the sole exception (they are trusted, so their location wins).
* Conflicts are never resolved by majority vote, by which copy was read first,
  or by CRC validity.  A record with any conflicting byte is a `conflict`;
  the CRC only judges records that were uniquely reconstructed.
* Byte positions belonging to no fragment are holes and are never invented.
* Overlapping fragments are compared at every shared position, even where a
  third fragment also covers the byte, so conflict detection does not depend
  on fragment order.
* Duplicated fragments with identical bytes are harmless: they agree.

The record parser reads the 4-byte length at the current record start.  If
those bytes are missing or conflicting, or decode to a length outside
1..65536, or the record would end past the sync segment, the whole rest of
the segment becomes one `boundary_unknown` line.  No payload bytes are scanned
for a "plausible" new boundary; only the next trusted sync point is used.

Memory stays bounded: bytes are fetched in <= 1 MiB chunks, only one record's
byte range is held at a time, and missing stretches are skipped by seeking
rather than scanned or filled.  In-memory state is fragment metadata (one
small record per fragment) plus disjoint recovered ranges; fragment bodies and
output lines are never accumulated.

Usage:
    python3 -I inspect_rebuild.py CATALOG [-o OUTPUT]

    CATALOG   JSON catalog (paths inside it are relative to its directory)
    OUTPUT    JSONL destination, or '-' for stdout (default: -).  A real path
              is written atomically: temp file + rename, so an existing file is
              left untouched on any failure.

Exit status:
    0  catalog valid, JSONL written (missing/conflict/invalid/boundary_unknown
       lines are normal deliverable content, not failures)
    2  invalid catalog, invalid/absent fragment, or a fragment that no longer
       matches its declared length
    1  output cannot be written, or an unexpected I/O/runtime failure
On any non-zero exit the output path is not created, truncated or replaced.
"""

from __future__ import annotations

import argparse
import bisect
import json
import os
import stat
import sys
import tempfile
import traceback
import zlib

MAX_PAYLOAD = 65536
MIN_PAYLOAD = 1
LENGTH_SIZE = 4
CRC_SIZE = 4
HEADER_SIZE = LENGTH_SIZE + CRC_SIZE
MAX_RECORD_BYTES = MAX_PAYLOAD + HEADER_SIZE
READ_CHUNK = 1 << 20
MAX_TOTAL_BYTES = (1 << 63) - 1


class CatalogError(Exception):
    """The catalog, or a fact it declares about a fragment, is invalid."""


class OutputError(Exception):
    """The output file could not be written."""


def _reject_constant(name):
    # Strict JSON has no NaN/Infinity.  Rejecting them keeps a "record" line
    # both genuinely JSON and re-serialisable.
    raise ValueError(f"non-finite JSON number {name!r} is not allowed")


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------

def _require_int(value, what):
    if type(value) is not int:
        raise CatalogError(
            f"{what} must be a JSON integer, got {type(value).__name__}")
    return value


def load_catalog(path):
    """Return (total_size, sync_points, [(abs_path, start, length), ...])."""
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
    except OSError as exc:
        raise CatalogError(
            f"cannot read catalog {path!r}: {exc.strerror or exc}") from exc
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CatalogError(f"catalog {path!r} is not valid UTF-8: {exc}") from exc
    try:
        obj = json.loads(text, parse_constant=_reject_constant)
    except ValueError as exc:
        raise CatalogError(f"catalog {path!r} is not valid JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise CatalogError("catalog must be a JSON object")

    for key in ("total_size", "sync_points", "fragments"):
        if key not in obj:
            raise CatalogError(f"catalog is missing '{key}'")

    total_size = _require_int(obj["total_size"], "'total_size'")
    if total_size < 0:
        raise CatalogError("'total_size' must not be negative")
    if total_size > MAX_TOTAL_BYTES:
        raise CatalogError("'total_size' exceeds the supported 64-bit range")

    raw_sync = obj["sync_points"]
    if not isinstance(raw_sync, list):
        raise CatalogError("'sync_points' must be a JSON array")
    sync_points = []
    for i, value in enumerate(raw_sync):
        value = _require_int(value, f"'sync_points[{i}]'")
        if i == 0 and value != 0:
            raise CatalogError("the first sync point must be 0")
        if i > 0 and value <= sync_points[-1]:
            raise CatalogError("'sync_points' must be strictly increasing")
        if value >= total_size:
            raise CatalogError(
                f"sync point {value} is not below 'total_size' {total_size}")
        sync_points.append(value)
    if not sync_points:
        if total_size != 0:
            raise CatalogError(
                "'sync_points' must contain at least the trusted start 0")
    elif total_size == 0:
        raise CatalogError("'sync_points' cannot be non-empty when 'total_size' is 0")

    raw_fragments = obj["fragments"]
    if not isinstance(raw_fragments, list):
        raise CatalogError("'fragments' must be a JSON array")

    catalog_dir = os.path.dirname(os.path.abspath(path))
    fragments = []
    for i, entry in enumerate(raw_fragments):
        where = f"'fragments[{i}]'"
        if not isinstance(entry, dict):
            raise CatalogError(f"{where} must be a JSON object")
        for key in ("path", "start", "length"):
            if key not in entry:
                raise CatalogError(f"{where} is missing '{key}'")
        rel = entry["path"]
        if not isinstance(rel, str) or not rel:
            raise CatalogError(f"{where}.path must be a non-empty string")
        start = _require_int(entry["start"], f"{where}.start")
        length = _require_int(entry["length"], f"{where}.length")
        if length <= 0:
            raise CatalogError(f"{where}.length must be positive")
        if start < 0:
            raise CatalogError(f"{where}.start must not be negative")
        if start + length > total_size:
            raise CatalogError(
                f"{where} spans [{start}, {start + length}) which exceeds "
                f"'total_size' {total_size}")
        full = rel if os.path.isabs(rel) else os.path.join(catalog_dir, rel)
        try:
            st = os.stat(full)
        except OSError as exc:
            raise CatalogError(
                f"cannot stat fragment ({where}) {rel!r}: "
                f"{exc.strerror or exc}") from exc
        if not stat.S_ISREG(st.st_mode):
            raise CatalogError(f"fragment {rel!r} is not a regular file")
        if st.st_size != length:
            raise CatalogError(
                f"fragment {rel!r} has {st.st_size} bytes but the catalog "
                f"declares {length}")
        fragments.append((full, start, length))

    return total_size, sync_points, fragments


# ---------------------------------------------------------------------------
# Fragment byte access
# ---------------------------------------------------------------------------

class HandleCache:
    """A few open fragment handles; fragments are immutable for one run."""

    __slots__ = ("limit", "handles", "order")

    def __init__(self, limit=16):
        self.limit = limit
        self.handles = {}
        self.order = []

    def read(self, path, offset, n):
        fh = self._handle(path)
        try:
            fh.seek(offset)
        except OSError as exc:
            raise CatalogError(
                f"cannot seek fragment {path!r}: {exc.strerror or exc}") from exc
        parts = []
        remaining = n
        while remaining:
            try:
                chunk = fh.read(min(remaining, READ_CHUNK))
            except OSError as exc:
                raise CatalogError(
                    f"cannot read fragment {path!r}: {exc.strerror or exc}") from exc
            if not chunk:
                break
            parts.append(chunk)
            remaining -= len(chunk)
        if remaining:
            raise CatalogError(
                f"fragment {path!r} shrank while running: wanted {n} bytes "
                f"at offset {offset}, got {n - remaining}")
        if len(parts) == 1:
            return parts[0]
        return b"".join(parts)

    def _handle(self, path):
        fh = self.handles.get(path)
        if fh is not None:
            return fh
        try:
            fh = open(path, "rb", buffering=0)
        except OSError as exc:
            raise CatalogError(
                f"cannot open fragment {path!r}: {exc.strerror or exc}") from exc
        if len(self.order) >= self.limit:
            old = self.order.pop(0)
            old_fh = self.handles.pop(old, None)
            if old_fh is not None:
                old_fh.close()
        self.handles[path] = fh
        self.order.append(path)
        return fh

    def close(self):
        for fh in self.handles.values():
            try:
                fh.close()
            except OSError:
                pass
        self.handles.clear()
        self.order.clear()


class FragmentStore:
    """Fragments sorted by logical start, with an interval-stabbing index."""

    __slots__ = ("starts", "ends", "paths", "n", "size", "maxend")

    def __init__(self, fragments):
        entries = sorted(fragments, key=lambda item: (item[1], item[2], item[0]))
        self.starts = [e[1] for e in entries]
        self.ends = [e[1] + e[2] for e in entries]
        self.paths = [e[0] for e in entries]
        self.n = len(entries)
        size = 1
        while size < self.n:
            size <<= 1
        self.size = size
        maxend = [-1] * (2 * size)
        for i, end in enumerate(self.ends):
            maxend[size + i] = end
        for i in range(size - 1, 0, -1):
            a, b = maxend[2 * i], maxend[2 * i + 1]
            maxend[i] = a if a >= b else b
        self.maxend = maxend

    def query(self, a, b):
        """Indices of all fragments whose [start, end) intersects [a, b)."""
        out = []
        if a >= b or self.n == 0:
            return out
        starts, ends = self.starts, self.ends
        maxend, n = self.maxend, self.n
        stack = [(1, 0, self.size)]
        while stack:
            node, lo, hi = stack.pop()
            if lo >= n:
                continue
            if starts[lo] >= b:          # leftmost (smallest) start in node
                continue
            if maxend[node] <= a:        # every interval ends at or before a
                continue
            if hi - lo == 1:
                out.append(lo)
                continue
            mid = (lo + hi) >> 1
            stack.append((2 * node + 1, mid, hi))
            stack.append((2 * node, lo, mid))
        out.sort()
        return out


class Coverage:
    """Merged union of all fragment ranges, for hole detection."""

    __slots__ = ("starts", "ends")

    def __init__(self, store):
        starts, ends = [], []
        for i in range(store.n):
            s, e = store.starts[i], store.ends[i]
            if starts and s <= ends[-1]:
                if e > ends[-1]:
                    ends[-1] = e
            else:
                starts.append(s)
                ends.append(e)
        self.starts = starts
        self.ends = ends

    def has_gap(self, a, b):
        """True if any position in [a, b) is covered by no fragment."""
        if a >= b:
            return False
        starts, ends = self.starts, self.ends
        n = len(starts)
        i = bisect.bisect_right(starts, a) - 1
        if i < 0:
            i = 0
        elif ends[i] <= a:
            i += 1
        cur = a
        while cur < b:
            if i >= n or starts[i] > cur:
                return True
            if ends[i] > cur:
                cur = ends[i]
            i += 1
        return False


def fetch(store, coverage, cache, a, b):
    """Classify [a, b) and, when unique/complete, return its bytes.

    Returns (status, data) with status in {'complete', 'gap', 'conflict'}.
    'conflict' outranks 'gap': one disagreeing copy anywhere is enough.

    A reference byte is taken from the first fragment covering each position
    and every other fragment is compared with it.  A position may be touched
    by fragments that never overlap each other, so comparing is done only
    against positions already filled; a fragment that merely abuts a filled
    region cannot fabricate a disagreement.
    """
    if a >= b:
        return "complete", b""
    gap = coverage.has_gap(a, b)
    indices = store.query(a, b)
    starts, ends, paths = store.starts, store.ends, store.paths
    width = b - a
    ref = bytearray(width)
    # Fragments arrive in increasing start order; positions before `lo` can no
    # longer be covered by any later fragment.  [a, filled_end) is decided.
    filled_end = a

    for idx in indices:
        lo = starts[idx] if starts[idx] > a else a
        hi = ends[idx] if ends[idx] < b else b
        if lo >= hi:
            continue
        data = cache.read(paths[idx], lo - starts[idx], hi - lo)
        if lo > filled_end:
            # A hole; later fragments start at or after lo, so it stays one.
            filled_end = lo
        overlap = hi if hi < filled_end else filled_end
        if overlap > lo:
            # Every position already decided here must hold the same value.
            if data[:overlap - lo] != ref[lo - a:overlap - a]:
                return "conflict", None
        if hi > filled_end:
            ref[filled_end - a:hi - a] = data[filled_end - lo:]
            filled_end = hi

    if gap:
        return "gap", None
    return "complete", bytes(ref)


# ---------------------------------------------------------------------------
# Record validation
# ---------------------------------------------------------------------------

def check_record(record):
    """Return ('record', object) or ('invalid', None) for complete bytes."""
    stored = int.from_bytes(record[-CRC_SIZE:], "little")
    payload = record[LENGTH_SIZE:-CRC_SIZE]
    if (zlib.crc32(payload) & 0xFFFFFFFF) != stored:
        return "invalid", None
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        return "invalid", None
    try:
        obj = json.loads(text, parse_constant=_reject_constant)
    except (ValueError, RecursionError):
        return "invalid", None
    if not isinstance(obj, dict):
        return "invalid", None
    return "record", obj


# ---------------------------------------------------------------------------
# Reconstruction
# ---------------------------------------------------------------------------

def parse_length(store, coverage, cache, pos, seg_end):
    """Return payload length, or None if this record boundary is untrusted."""
    if pos + LENGTH_SIZE > seg_end:
        return None
    status, data = fetch(store, coverage, cache, pos, pos + LENGTH_SIZE)
    if status != "complete":
        return None
    length = int.from_bytes(data, "little")
    if length < MIN_PAYLOAD or length > MAX_PAYLOAD:
        return None
    return length


def reconstruct(total_size, sync_points, store, coverage, cache, emit):
    """Walk trusted segments, calling emit(type, start, end, object) per line."""
    if total_size == 0:
        return
    bounds = list(sync_points)
    bounds.append(total_size)
    for i in range(len(bounds) - 1):
        seg_start = bounds[i]
        seg_end = bounds[i + 1]
        pos = seg_start
        while pos < seg_end:
            length = parse_length(store, coverage, cache, pos, seg_end)
            if length is None:
                emit("boundary_unknown", pos, seg_end, None)
                pos = seg_end
                continue
            end = pos + HEADER_SIZE + length
            if end > seg_end:
                # Would cross a trusted sync point: the length cannot be
                # believed, and no later boundary in this segment is known.
                emit("boundary_unknown", pos, seg_end, None)
                pos = seg_end
                continue
            status, record = fetch(store, coverage, cache, pos, end)
            if status == "conflict":
                emit("conflict", pos, end, None)
            elif status == "gap":
                emit("missing", pos, end, None)
            else:
                kind, obj = check_record(record)
                if kind == "record":
                    emit("record", pos, end, obj)
                else:
                    emit("invalid", pos, end, None)
            pos = end


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

class OutputWriter:
    """Line emitter; a real path is written atomically via temp + rename."""

    __slots__ = ("target", "tmp_path", "fh", "committed", "dirpath")

    def __init__(self, target):
        self.target = target
        self.tmp_path = None
        self.fh = None
        self.committed = False
        self.dirpath = None
        if target == "-":
            self.fh = sys.stdout
            return
        directory = os.path.dirname(os.path.abspath(target))
        base = os.path.basename(target)
        self.dirpath = directory
        try:
            fd, tmp_path = tempfile.mkstemp(
                prefix=f".{base}.", suffix=".tmp", dir=directory)
            self.tmp_path = tmp_path
            self.fh = os.fdopen(fd, "w", encoding="utf-8", newline="\n")
        except OSError as exc:
            raise OutputError(
                f"cannot create temporary output next to {target!r}: "
                f"{exc.strerror or exc}") from exc

    def line(self, kind, start, end, obj):
        if obj is None:
            record = {"type": kind, "start": start, "end": end}
        else:
            record = {"type": kind, "start": start, "end": end, "object": obj}
        try:
            self.fh.write(json.dumps(
                record, ensure_ascii=True, separators=(",", ":")))
            self.fh.write("\n")
        except OSError as exc:
            raise OutputError(
                f"cannot write output: {exc.strerror or exc}") from exc

    def commit(self):
        if self.committed:
            return
        if self.tmp_path is None:
            try:
                self.fh.flush()
            except OSError as exc:
                raise OutputError(
                    f"cannot write output: {exc.strerror or exc}") from exc
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
            raise OutputError(
                f"cannot finalise output {self.target!r}: "
                f"{exc.strerror or exc}") from exc
        try:
            dfd = os.open(self.dirpath, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError:
            pass
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


# ---------------------------------------------------------------------------
# Command
# ---------------------------------------------------------------------------

def run(catalog_path, output_path):
    total_size, sync_points, fragments = load_catalog(catalog_path)
    store = FragmentStore(fragments)
    coverage = Coverage(store)
    cache = HandleCache()
    writer = None
    try:
        writer = OutputWriter(output_path)
        reconstruct(total_size, sync_points, store, coverage, cache,
                    writer.line)
        writer.commit()
    except BaseException:
        if writer is not None:
            writer.abort()
        raise
    finally:
        cache.close()


def build_parser():
    parser = argparse.ArgumentParser(
        prog="inspect_rebuild.py",
        description="Reconstruct a position-ordered JSONL view of a damaged "
                    "binary inspection-record stream.")
    parser.add_argument("catalog", help="JSON catalog describing the stream "
                                        "and its local fragments")
    parser.add_argument("-o", "--output", default="-",
                        help="JSONL destination; '-' means stdout (default)")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        run(args.catalog, args.output)
    except CatalogError as exc:
        print(f"inspect_rebuild: invalid input: {exc}", file=sys.stderr)
        return 2
    except OutputError as exc:
        print(f"inspect_rebuild: output failure: {exc}", file=sys.stderr)
        return 1
    except BrokenPipeError:
        return 1
    except Exception:
        print("inspect_rebuild: unexpected failure:", file=sys.stderr)
        traceback.print_exc()
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
