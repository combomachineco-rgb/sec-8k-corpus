"""Download every SEC Form 8-K in the window, whole: body and all exhibits.

One request per filing: EDGAR's complete submission text file
  https://www.sec.gov/Archives/edgar/data/{cik}/{acc-nodash}/{accession}.txt
which carries the header (acceptance time, form type, items) and every
document of the filing as <DOCUMENT> blocks. Every text document
(.htm/.html/.txt) is kept in full: tags stripped, entities decoded, table
rows kept, no length cap. Binary documents (graphics, PDF, XBRL, zip) are
not stored but are listed per filing in `docs_skipped` so nothing vanishes
silently.

Input:   filings_index.json.gz  (EDGAR nightly submissions snapshot:
         accession -> [cik, primaryDocument, items, filingDate]; 281,935 rows,
         filing dates 2022-07-18 .. 2026-08-07)
Output:  data/{YYYY}/{YYYY-MM-DD}.jsonl.gz  one row per stored DOCUMENT
         data/{YYYY}/{YYYY-MM}.done.txt      accessions finished (resume index)
         data/{YYYY}/{YYYY-MM}.failed.jsonl  fetch failures; retried next run

Sharding: months are dealt round-robin across `--of` shards, so parallel
jobs never write the same file. Resume: a shard reads its months' done files
and skips finished accessions.

Rate: a global gate keeps >= SEC_SLEEP seconds between consecutive requests
from this process across all threads (SEC asks for <= 10 req/s; this holds
<= 6.7). A 429/503 pauses every thread for Retry-After (or backoff).
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import html as htmllib
import json
import re
import subprocess
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
INDEX = ROOT / "filings_index.json.gz"
OUT = ROOT / "data"
UA = "sec-8k-corpus research contact systain11@gmail.com"
SEC_SLEEP = 0.15
TEXT_EXT = (".htm", ".html", ".txt")
FETCHER_REV = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT,
                             capture_output=True, text=True).stdout.strip() or "unknown"

# ------------------------------------------------------------------ network
_gate = threading.Lock()
_next_slot = 0.0
_pause_until = 0.0
_session = requests.Session()
_session.headers.update({"User-Agent": UA, "Accept-Encoding": "gzip, deflate"})


def sec_get(url: str, tries: int = 6) -> requests.Response:
    global _next_slot, _pause_until
    delay = 2.0
    for attempt in range(tries):
        with _gate:
            now = time.monotonic()
            wait = max(_next_slot, _pause_until) - now
            _next_slot = max(now, _next_slot, _pause_until) + SEC_SLEEP
        if wait > 0:
            time.sleep(wait)
        try:
            r = _session.get(url, timeout=120)
        except requests.RequestException:
            if attempt == tries - 1:
                raise
            time.sleep(delay); delay = min(delay * 2, 60.0)
            continue
        if r.status_code in (429, 503):
            ra = r.headers.get("Retry-After")
            try:
                pause = float(ra) if ra else delay
            except ValueError:
                pause = delay
            pause = min(max(pause, delay), 120.0)
            with _gate:
                _pause_until = max(_pause_until, time.monotonic() + pause)
            delay = min(delay * 2, 60.0)
            if attempt == tries - 1:
                r.raise_for_status()
            continue
        r.raise_for_status()
        return r
    raise RuntimeError("unreachable")


# ------------------------------------------------------------------ parsing
_DOC_RE = re.compile(r"<DOCUMENT>(.*?)</DOCUMENT>", re.S)
_TEXT_RE = re.compile(r"<TEXT>\s*(.*?)\s*</TEXT>", re.S)
_SCRIPT_RE = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.S | re.I)
_BLOCK_RE = re.compile(r"</(?:tr|p|div|li|h[1-6]|td|th|table|blockquote|pre)>|<br\s*/?>|<hr\s*/?>", re.I)
_TAG_RE = re.compile(r"<[^>]+>")


def _tag(block: str, name: str) -> str:
    m = re.search(rf"^<{name}>(.*)$", block, re.M)
    return m.group(1).strip() if m else ""


def _hdr(header: str, label: str) -> str:
    m = re.search(rf"^\s*{re.escape(label)}:\s*(.*)$", header, re.M)
    return m.group(1).strip() if m else ""


def html_to_text(raw: str) -> str:
    s = _SCRIPT_RE.sub(" ", raw)
    s = _BLOCK_RE.sub("\n", s)
    s = _TAG_RE.sub(" ", s)
    s = htmllib.unescape(s).replace("\xa0", " ")
    lines = [re.sub(r"[ \t\r\f\v]+", " ", ln).strip() for ln in s.split("\n")]
    return "\n".join(ln for ln in lines if ln)


def parse_submission(sub: str) -> tuple[dict, list[dict], list[dict]]:
    """-> (header facts, stored docs, skipped docs)."""
    hm = re.search(r"<SEC-HEADER>(.*?)</SEC-HEADER>", sub, re.S)
    header = hm.group(1) if hm else sub[:5000]
    facts = {
        "form_type": _hdr(header, "CONFORMED SUBMISSION TYPE"),
        "acceptance_datetime": (re.search(r"<ACCEPTANCE-DATETIME>(\d+)", sub) or [None, ""])[1],
        "period_of_report": _hdr(header, "CONFORMED PERIOD OF REPORT"),
        "company_name": _hdr(header, "COMPANY CONFORMED NAME"),
        "items_declared": re.findall(r"^\s*ITEM INFORMATION:\s*(.*)$", header, re.M),
    }
    docs, skipped = [], []
    for block in _DOC_RE.findall(sub):
        dtype, fname = _tag(block, "TYPE"), _tag(block, "FILENAME")
        seq, desc = _tag(block, "SEQUENCE"), _tag(block, "DESCRIPTION")
        meta = {"seq": int(seq) if seq.isdigit() else None, "doc_type": dtype,
                "filename": fname, "description": desc}
        if not fname.lower().endswith(TEXT_EXT) or dtype.upper() == "GRAPHIC":
            skipped.append(meta)
            continue
        tm = _TEXT_RE.search(block)
        body = tm.group(1) if tm else ""
        text = html_to_text(body) if fname.lower().endswith((".htm", ".html")) else \
            "\n".join(ln.rstrip() for ln in body.split("\n")).strip()
        docs.append({**meta, "chars": len(text),
                     "sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
                     "text": text})
    return facts, docs, skipped


def fetch_filing(acc: str, meta: list) -> tuple[str, list[dict], dict | None]:
    """-> (month, rows, failure)."""
    cik, primary, items, fdate = meta
    url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc.replace('-', '')}/{acc}.txt"
    base = {"accession": acc, "cik": cik, "filing_date": fdate, "primary_document": primary,
            "items_index": [x for x in items.split(",") if x], "source_url": url,
            "fetcher_revision": FETCHER_REV,
            "fetched_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    try:
        r = sec_get(url)
        sub = r.content.decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001
        return fdate[:7], [], {**base, "error": f"{type(exc).__name__}: {exc}"[:200]}
    facts, docs, skipped = parse_submission(sub)
    if not docs:
        return fdate[:7], [], {**base, "error": f"no text documents parsed ({len(sub)} bytes)"}
    rows = [{**base, **facts, "n_docs": len(docs), "docs_skipped": skipped,
             "submission_bytes": len(r.content), **d} for d in docs]
    return fdate[:7], rows, None


# ------------------------------------------------------------------ storage
def day_path(date: str) -> Path:
    return OUT / date[:4] / f"{date}.jsonl.gz"


def done_path(month: str) -> Path:
    return OUT / month[:4] / f"{month}.done.txt"


def failed_path(month: str) -> Path:
    return OUT / month[:4] / f"{month}.failed.jsonl"


def load_done(months: list[str]) -> set[str]:
    done: set[str] = set()
    for m in months:
        p = done_path(m)
        if p.exists():
            done.update(x.strip() for x in p.read_text().splitlines() if x.strip())
    return done


def append(pending_rows: dict[str, list[dict]], pending_done: dict[str, list[str]],
           pending_failed: dict[str, list[dict]]) -> None:
    for date, rows in pending_rows.items():
        p = day_path(date); p.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(p, "at", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    for month, accs in pending_done.items():
        p = done_path(month); p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a") as fh:
            fh.write("".join(a + "\n" for a in accs))
    for month, fails in pending_failed.items():
        p = failed_path(month); p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("a") as fh:
            for f in fails:
                fh.write(json.dumps(f) + "\n")


def git_commit_push(branch: str, msg: str) -> None:
    def run(*a):
        return subprocess.run(["git", *a], cwd=ROOT, capture_output=True, text=True)
    run("add", "-A", str(OUT))
    if run("diff", "--cached", "--quiet").returncode == 0:
        return
    run("commit", "-q", "-m", msg)
    for i in range(8):
        if run("push", "origin", f"HEAD:{branch}").returncode == 0:
            return
        run("pull", "--rebase", "--autostash", "origin", branch)
        time.sleep(5 * (i + 1))
    print("push failed after retries", flush=True)


# ------------------------------------------------------------------ main
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shard", type=int, default=0)
    ap.add_argument("--of", type=int, default=1)
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--budget-min", type=float, default=330.0)
    ap.add_argument("--commit-every-min", type=float, default=20.0)
    ap.add_argument("--branch", default="")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    idx = json.loads(gzip.decompress(INDEX.read_bytes()))
    months = sorted({v[3][:7] for v in idx.values()})
    mine = [m for i, m in enumerate(months) if i % args.of == args.shard]
    done = load_done(mine)
    todo = sorted(((a, v) for a, v in idx.items() if v[3][:7] in mine and a not in done),
                  key=lambda kv: kv[1][3])
    if args.limit:
        todo = todo[:args.limit]
    total_mine = sum(1 for v in idx.values() if v[3][:7] in mine)
    print(f"shard {args.shard}/{args.of}: months={len(mine)} filings={total_mine} "
          f"done={len(done)} todo={len(todo)} rev={FETCHER_REV}", flush=True)

    t0 = time.monotonic(); last_commit = t0
    p_rows: dict[str, list[dict]] = defaultdict(list)
    p_done: dict[str, list[str]] = defaultdict(list)
    p_fail: dict[str, list[dict]] = defaultdict(list)
    stats = defaultdict(int); n = 0

    def flush(final: bool = False):
        nonlocal last_commit
        append(p_rows, p_done, p_fail)
        p_rows.clear(); p_done.clear(); p_fail.clear()
        # First commit early (proof the run works lands within minutes), then every commit_every_min.
        every = 3.0 if last_commit == t0 else args.commit_every_min
        if args.branch and (final or time.monotonic() - last_commit > every * 60):
            git_commit_push(args.branch, f"shard {args.shard}/{args.of}: +{n} filings {dict(stats)}")
            last_commit = time.monotonic()

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        it = iter(todo); futures = {}
        def submit_next():
            if time.monotonic() - t0 > args.budget_min * 60:
                return
            try:
                a, v = next(it)
            except StopIteration:
                return
            futures[ex.submit(fetch_filing, a, v)] = a
        for _ in range(args.workers * 2):
            submit_next()
        while futures:
            for f in list(futures):
                if not f.done():
                    continue
                acc = futures.pop(f)
                month, rows, fail = f.result()
                if fail:
                    p_fail[month].append(fail); stats["failed"] += 1
                    if n < 50 and stats["failed"] == n + 1 and n + 1 >= 20:
                        # FAIL FAST: the first 20 filings all failed -> something
                        # systematic (URL, network, parser), not per-filing noise.
                        print(f"ABORT: first {n+1} filings all failed; last error: {fail['error']}", flush=True)
                        flush(final=True); sys.exit(2)
                else:
                    p_rows[rows[0]["filing_date"]].extend(rows)
                    p_done[month].append(acc)
                    stats["filings"] += 1; stats["docs"] += len(rows)
                    stats["mb_text"] = round(stats["mb_text"] + sum(r["chars"] for r in rows) / 1e6, 1)
                n += 1
                if n % 500 == 0:
                    el = time.monotonic() - t0
                    print(f"{n}/{len(todo)} {el/60:.1f}min {n/el:.2f} filings/s {dict(stats)}", flush=True)
                if n % 200 == 0:
                    flush()
                submit_next()
            time.sleep(0.05)
    flush(final=True)
    el = time.monotonic() - t0
    print(f"DONE shard {args.shard}/{args.of}: {n} filings in {el/60:.1f} min, {dict(stats)}, "
          f"remaining={len(todo) - n}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
