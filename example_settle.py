#!/usr/bin/env python3
"""Runnable example for the `settle` export of shift_ledger.py.

Builds a tiny ledger (one pump, five readings, one corrected counter and
one counter reset across an epoch change) plus a dispatch timeline in a
temporary directory, then exports the settlement manifest for one work
window and prints it next to the hand-computed expectation.

Run:  python3 -I example_settle.py
"""

import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
TOOL = os.path.join(HERE, "shift_ledger.py")


def run(*args):
    result = subprocess.run([sys.executable, "-I", TOOL, *args],
                            capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(f"command failed: {' '.join(args)}\n"
                         f"{result.stderr}")
    return result


def record(obj):
    return json.dumps({"type": "record", "start": 0, "end": 1,
                       "object": obj})


def main():
    tmp = tempfile.mkdtemp(prefix="settle-example-")
    db = os.path.join(tmp, "ledger.db")

    # Pump P-7 cumulative-counter readings.  a2 is corrected once at a
    # higher revision; a4 opens a new epoch with a reset counter.
    readings = [
        {"device": "P-7", "reading_id": "a1", "revision": 1, "op": "put",
         "epoch": "2026-W47", "time": 1000, "counter": 800},
        {"device": "P-7", "reading_id": "a2", "revision": 1, "op": "put",
         "epoch": "2026-W47", "time": 1600, "counter": 900},
        {"device": "P-7", "reading_id": "a2", "revision": 2, "op": "put",
         "epoch": "2026-W47", "time": 1600, "counter": 920},
        {"device": "P-7", "reading_id": "a3", "revision": 1, "op": "put",
         "epoch": "2026-W47", "time": 2400, "counter": 980},
        {"device": "P-7", "reading_id": "a4", "revision": 1, "op": "put",
         "epoch": "2026-W48", "time": 3000, "counter": 5},
        {"device": "P-7", "reading_id": "a5", "revision": 1, "op": "put",
         "epoch": "2026-W48", "time": 3600, "counter": 40},
    ]
    batch = os.path.join(tmp, "readings.jsonl")
    with open(batch, "w", encoding="utf-8") as fh:
        for obj in readings:
            fh.write(record(obj) + "\n")
    run("apply", db, batch)

    # The dispatch facts this settlement run believes in.  WO-100 is
    # dispatched twice concurrently (counts as one job); WO-101 abuts it
    # exactly; nothing is dispatched at or after second 2400.
    dispatch = [
        {"device": "P-7", "job": "WO-100", "start": 900, "end": 1700},
        {"device": "P-7", "job": "WO-100", "start": 1000, "end": 1600},
        {"device": "P-7", "job": "WO-101", "start": 1700, "end": 2400},
    ]
    assign = os.path.join(tmp, "dispatch.jsonl")
    with open(assign, "w", encoding="utf-8") as fh:
        for obj in dispatch:
            fh.write(json.dumps(obj) + "\n")

    out = os.path.join(tmp, "settle.jsonl")
    run("settle", db, "--from", "0", "--to", "4000",
        "--assign", assign, "-o", out, "P-7")

    print(f"ledger:    {db}")
    print(f"manifest:  {out}\n")
    with open(out, encoding="utf-8") as fh:
        rows = [json.loads(line) for line in fh if line.strip()]
    for row in rows:
        print(json.dumps(row, ensure_ascii=False))

    print("""
reading the manifest:
  [1000,1600)  +120  allocated  WO-100 (two concurrent rows count once;
                                the corrected a2@rev2 is the evidence)
  [1600,2400)  +60   handoff    WO-100 hands over to WO-101 at 1700
  [2400,3000)        unmeasurable  epoch change + counter reset
  [3000,3600)  +35   uncovered  no dispatch on record at all
Only overlap/uncovered/handoff/unmeasurable need a human; the allocated
line can be settled against WO-100 directly.""")


if __name__ == "__main__":
    main()
