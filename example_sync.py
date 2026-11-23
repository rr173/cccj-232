#!/usr/bin/env python3
"""Runnable end-to-end example of offline replica synchronisation.

The script never touches a network: every transfer is a file handed
between a centre database and a branch replica inside one temporary
directory.  It walks the whole story:

  1. an already-populated "old" centre ledger is brought under sync and
     a blank branch replica is connected from position 0;
  2. a reading sitting exactly on a shift boundary is retracted, so the
     amount that used to be split cleanly across two shifts becomes one
     cross-shift uncertain interval, while the other endpoint only gets
     a revision bump with the same value;
  3. the two shifts are recomputed by hand from the raw readings and
     checked against the replica, including the evidence revisions
     cited by its new settlement manifest;
  4. a skipped (gap) pack is refused, a pack cut down to its first
     legal frames is detected as truncated and advances nothing, then
     the good pack is accepted and replayed byte-identically as a
     no-op;
  5. an apply landing while a pack is being built is shown to belong to
     one complete committed version (this pack or the next);
  6. a long generated history proves a small update packs as one row
     without scanning the unchanged history.

Run:  python3 -I example_sync.py
"""

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
TOOL = os.path.join(HERE, "shift_ledger.py")
SHIFT = 28800


def run(*args, expect=0):
    result = subprocess.run([sys.executable, "-I", TOOL, *args],
                            capture_output=True, text=True)
    if result.returncode != expect:
        raise SystemExit(
            f"command failed ({result.returncode}): {' '.join(args)}\n"
            f"{result.stderr}")
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


def write_batch(path, messages):
    with open(path, "w", encoding="utf-8") as fh:
        for m in messages:
            fh.write(record(m) + "\n")


def query(db, device, start, end):
    out = run("query", db, device, str(start), str(end)).stdout
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def position(db, table):
    conn = sqlite3.connect(db)
    try:
        return conn.execute(
            f"SELECT position FROM {table} WHERE id = 1").fetchone()[0]
    finally:
        conn.close()


def banner(title):
    print("\n" + "=" * 72 + f"\n{title}\n" + "=" * 72)


def main():
    tmp = tempfile.mkdtemp(prefix="sync-example-")
    center = os.path.join(tmp, "center.db")
    replica = os.path.join(tmp, "replica.db")
    print(f"work directory: {tmp}")

    # ------------------------------------------------------------------
    # 1. Bring an already-populated centre ledger under sync.
    # ------------------------------------------------------------------
    banner("1. Old centre ledger -> first sync, blank replica connects")
    # Pump P-1: r1 inside shift 0, r3 exactly on the shift boundary,
    # r2 inside shift 1.  All same epoch, non-falling counters.
    initial = [
        put("P-1", "r1", 1, "2026-W47", 28000, 100),
        put("P-1", "r3", 1, "2026-W47", 28800, 180),  # on the boundary
        put("P-1", "r2", 1, "2026-W47", 30000, 250),
    ]
    batch1 = os.path.join(tmp, "batch1.jsonl")
    write_batch(batch1, initial)
    run("apply", center, batch1)
    msg = run("sync-init", center).stderr.strip()
    print(msg)
    assert "position=1" in msg, "existing data must snapshot as version 1"
    pack0 = os.path.join(tmp, "pack-0-to-1.slpack")
    run("sync-pack", center, "--base", "0", "-o", pack0)
    run("sync-receive", replica, pack0)
    print(f"replica position after first receive: "
          f"{position(replica, 'sync_replica')}")
    before = query(replica, "P-1", 0, 2 * SHIFT)
    print("replica shifts before the change:")
    for row in before:
        print(" ", json.dumps(row))

    # Hand calculation, version 1:
    #   [28000,28800): +80, fully inside shift 0 -> shift 0 confirmed 80
    #   [28800,30000): +70, fully inside shift 1 -> shift 1 confirmed 70
    assert before[0]["confirmed_increment"] == 80
    assert before[1]["confirmed_increment"] == 70
    assert all(r["uncertain_seconds"] == 0 for r in before)

    # ------------------------------------------------------------------
    # 2. Retract the boundary point; bump the other endpoint's revision
    #    only (same value).
    # ------------------------------------------------------------------
    banner("2. Retract boundary reading r3; r2 gets a revision-only bump")
    batch2 = os.path.join(tmp, "batch2.jsonl")
    write_batch(batch2, [
        delete("P-1", "r3", 2),                       # tombstone, carried
        put("P-1", "r2", 2, "2026-W47", 30000, 250),  # version moves, value
    ])                                               # does not
    run("apply", center, batch2)
    print(f"centre position is now {position(center, 'sync_center')} "
          f"(one apply, one advance even though r2's value is unchanged)")
    pack1 = os.path.join(tmp, "pack-1-to-2.slpack")
    run("sync-pack", center, "--base", "1", "-o", pack1)
    # The pack names only the two identities that changed, one row each.
    sys.path.insert(0, HERE)
    import shift_ledger as sl
    header, rows, _ = sl.verify_pack(pack1)
    print(f"pack {header['base']} -> {header['end']} carries {rows} "
          f"identity row(s): r3's tombstone and r2's new revision")
    assert rows == 2

    # ------------------------------------------------------------------
    # 3. Gap refusal, truncation refusal, then the good pack.
    # ------------------------------------------------------------------
    banner("3. Gap pack refused; truncated pack advances nothing")
    # Pretend pack-1-to-2 was lost in transit and the centre already
    # moved on: build the next pack and try to skip the gap.
    write_batch(os.path.join(tmp, "batch3.jsonl"),
                [put("P-1", "r1", 2, "2026-W47", 28000, 105)])
    run("apply", center, os.path.join(tmp, "batch3.jsonl"))
    pack2 = os.path.join(tmp, "pack-2-to-3.slpack")
    run("sync-pack", center, "--base", "2", "-o", pack2)
    gap = run("sync-receive", replica, pack2, expect=2)
    print("gap attempt ->", gap.stderr.strip())
    assert position(replica, "sync_replica") == 1
    # Truncate the good pack to its first legal frames plus a cut tail.
    size = os.path.getsize(pack1)
    cut = os.path.join(tmp, "pack-1-to-2.truncated")
    with open(pack1, "rb") as fh:
        blob = fh.read()
    with open(cut, "wb") as fh:
        fh.write(blob[:size - 12])
    truncated = run("sync-receive", replica, cut, expect=2)
    print("truncated pack ->", truncated.stderr.strip())
    assert position(replica, "sync_replica") == 1
    assert query(replica, "P-1", 0, 2 * SHIFT) == before  # untouched

    banner("4. Good pack accepted; hand-checked shifts; replay is a no-op")
    run("sync-receive", replica, pack1)
    after = query(replica, "P-1", 0, 2 * SHIFT)
    print("replica shifts after the retraction:")
    for row in after:
        print(" ", json.dumps(row))
    # Hand calculation, version 2: only r1@28000 (counter 100) and
    # r2@30000 (counter 250) remain.  Interval [28000,30000), diff 150,
    # crosses the 28800 boundary: positive cross-shift differences are
    # never apportioned.
    #   shift 0 touches [28000,28800): 800 uncertain seconds, confirmed 0
    #   shift 1 touches [28800,30000): 1200 uncertain seconds, confirmed 0
    assert after[0]["confirmed_increment"] == 0
    assert after[0]["uncertain_seconds"] == 800
    assert after[1]["confirmed_increment"] == 0
    assert after[1]["uncertain_seconds"] == 1200
    assert position(replica, "sync_replica") == 2

    # The replica's settlement manifest cites the surviving endpoint
    # revisions, including r2's bumped revision 2.
    assign = os.path.join(tmp, "assign.jsonl")
    with open(assign, "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"device": "P-1", "job": "WO-7",
                             "start": 28000, "end": 30000}) + "\n")
    manifest = os.path.join(tmp, "settle.jsonl")
    run("settle", replica, "--from", "0", "--to", "40000",
        "--assign", assign, "-o", manifest, "P-1")
    with open(manifest, encoding="utf-8") as fh:
        lines = [json.loads(x) for x in fh if x.strip()]
    print("replica settlement evidence:")
    for line in lines:
        print(" ", json.dumps(line))
    assert [(l["left_id"], l["left_revision"], l["right_id"],
             l["right_revision"]) for l in lines] == [
        ("r1", 1, "r2", 2)]
    assert lines[0]["status"] == "allocated" and lines[0]["amount"] == 150

    # Byte-identical re-delivery of the accepted file: success, no move.
    replay = run("sync-receive", replica, pack1)
    print("replay ->", replay.stderr.strip())
    assert position(replica, "sync_replica") == 2
    # The skipped pack can now never be applied: its base 2 equals the
    # replica position, but only after pack-2-to-3's real predecessor
    # chain would fit; here pack2's base is 2 and replica is at 2, so
    # it applies cleanly and completes the catch-up.
    run("sync-receive", replica, pack2)
    print(f"replica caught up to position "
          f"{position(replica, 'sync_replica')}")
    assert position(replica, "sync_replica") == 3

    # ------------------------------------------------------------------
    # 5. An apply during packing maps to one complete version.
    # ------------------------------------------------------------------
    banner("5. Apply landing during a pack stays in one committed version")
    import shift_ledger as sl
    lock = sl.connect(center)
    lock.execute("BEGIN IMMEDIATE")          # hold the write lock
    late_batch = os.path.join(tmp, "late.jsonl")
    write_batch(late_batch, [put("P-1", "r9", 1, "2026-W47", 31000, 260)])
    holder = {}
    thread = threading.Thread(
        target=lambda: holder.update(
            rc=sl.apply_batch(center, late_batch)))
    thread.start()
    time.sleep(0.5)                         # writer is queued on the lock
    snap_pack = os.path.join(tmp, "snapshot.slpack")
    run("sync-pack", center, "--base", "0", "-o", snap_pack)
    header, rows, _ = sl.verify_pack(snap_pack)
    print(f"pack built while the writer waits: end={header['end']}, "
          f"rows={rows} (r9 is not in it)")
    lock.commit()                            # release; writer proceeds
    thread.join(10)
    assert "rc" in holder
    header2, rows2, _ = sl.verify_pack(snap_pack)
    assert header2["end"] == 3
    next_pack = os.path.join(tmp, "next.slpack")
    run("sync-pack", center, "--base", "3", "-o", next_pack)
    header3, rows3, _ = sl.verify_pack(next_pack)
    print(f"the queued apply landed in the next pack: "
          f"{header3['base']} -> {header3['end']}, {rows3} row(s)")
    assert (header3["base"], header3["end"], rows3) == (3, 4, 1)

    # ------------------------------------------------------------------
    # 6. Long history: a small update never scans the unchanged part.
    # ------------------------------------------------------------------
    banner("6. Long generated history; small update packs as one row")
    big_center = os.path.join(tmp, "big-center.db")
    big_rep = os.path.join(tmp, "big-replica.db")
    run("sync-init", big_center)
    n = 20000
    for lo in range(0, n, 5000):
        write_batch(os.path.join(tmp, f"big-{lo}.jsonl"), [
            put("H", f"r{i}", 1, "E", 1000 + i, i)
            for i in range(lo, min(lo + 5000, n))])
        run("apply", big_center, os.path.join(tmp, f"big-{lo}.jsonl"))
    full = os.path.join(tmp, "big-full.slpack")
    run("sync-pack", big_center, "--base", "0", "-o", full)
    run("sync-receive", big_rep, full)
    write_batch(os.path.join(tmp, "big-tiny.jsonl"),
                [put("H", "r12345", 2, "E", 1000 + 12345, 999999)])
    run("apply", big_center, os.path.join(tmp, "big-tiny.jsonl"))
    started = time.monotonic()
    tiny = os.path.join(tmp, "big-tiny.slpack")
    run("sync-pack", big_center, "--base", "4", "-o", tiny)
    elapsed = time.monotonic() - started
    header4, rows4, _ = sl.verify_pack(tiny)
    print(f"one reading changed among {n}: pack 4 -> {header4['end']} "
          f"with {rows4} business row(s), built in {elapsed:.3f}s")
    assert rows4 == 1
    run("sync-receive", big_rep, tiny)
    # Centre and replica now hold identical ledger content.
    def dump(db):
        conn = sqlite3.connect(db)
        try:
            return sorted(conn.execute(
                "SELECT device, reading_id, revision, op, epoch, time, "
                "counter FROM readings").fetchall())
        finally:
            conn.close()
    assert dump(big_center) == dump(big_rep)
    print("centre and replica readings tables are identical")

    print("\nAll example checks passed.")
    print(f"Every artefact is kept under {tmp} for inspection.")


if __name__ == "__main__":
    main()
