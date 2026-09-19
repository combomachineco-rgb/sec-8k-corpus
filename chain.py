"""Round bookkeeping for the self-chaining fetch workflow.

Counts finished accessions (data/*/*.done.txt) against the index, records
the round in data/STATUS.json, and tells the workflow whether to dispatch
another round: yes while filings remain, the round limit is not reached,
and this round made progress. Failed-lists are informational here; the
fetcher retries anything not in a done-list on the next round.
"""
from __future__ import annotations

import argparse
import glob
import gzip
import json
import os
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
STATUS = ROOT / "data" / "STATUS.json"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--round", type=int, required=True)
    ap.add_argument("--max-rounds", type=int, required=True)
    ap.add_argument("--fetch-result", default="")
    a = ap.parse_args()

    idx = json.loads(gzip.decompress((ROOT / "filings_index.json.gz").read_bytes()))
    total = len(idx)
    done: set[str] = set()
    for p in glob.glob(str(ROOT / "data" / "*" / "*.done.txt")):
        done.update(x.strip() for x in Path(p).read_text().splitlines() if x.strip())
    done &= set(idx)
    failed_rows = sum(1 for p in glob.glob(str(ROOT / "data" / "*" / "*.failed.jsonl"))
                      for _ in open(p))
    remaining = total - len(done)

    prev = json.loads(STATUS.read_text()) if STATUS.exists() else {}
    progress = len(done) - int(prev.get("done", 0))
    reason = ""
    if remaining == 0:
        reason = "complete"
    elif a.round >= a.max_rounds:
        reason = f"round limit {a.max_rounds} reached"
    elif a.round > 1 and progress <= 0:
        reason = "no progress this round"
    nxt = reason == ""

    status = {
        "round": a.round, "fetch_job_result": a.fetch_result,
        "total": total, "done": len(done), "remaining": remaining,
        "progress_this_round": progress, "failed_rows": failed_rows,
        "next_round": nxt, "stopped_because": reason or None,
        "updated_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "history": (prev.get("history") or []) + [{"round": a.round, "done": len(done),
                                                    "remaining": remaining}],
    }
    STATUS.parent.mkdir(parents=True, exist_ok=True)
    STATUS.write_text(json.dumps(status, indent=2) + "\n")
    print(json.dumps(status, indent=2))
    out = os.environ.get("GITHUB_OUTPUT")
    if out:
        with open(out, "a") as fh:
            fh.write(f"next={'true' if nxt else 'false'}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
