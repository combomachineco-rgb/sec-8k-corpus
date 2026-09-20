"""Verify corpus.db against its sources. Exit 0 only if EVERY check passes.

  1. integrity_check          SQLite reports ok
  2. filings == index         every accession in filings_index.json.gz is a filings row, and nothing else
  3. documents == day files   total document rows equals the number of lines across data/*/*.jsonl.gz
  4. per-filing doc counts    documents rows per accession == n_docs recorded at fetch time
  5. text fidelity (ALL rows) sha256(text) recomputed == sha256 stored at fetch time, for every document
  6. items mapping            no item_code '?', and every filing with a declared item has item rows
  7. dates                    every filing_date inside 2022-07-18..2026-08-07 and matches the index
  8. per-month counts         filings per month == index per month
"""
import glob, gzip, hashlib, json, sqlite3, sys, collections
from pathlib import Path
ROOT = Path(__file__).resolve().parent
DB = ROOT / "corpus.db"
fails = []
def check(name, ok, detail=""):
    print(("PASS " if ok else "FAIL ") + name + ("  " + detail if detail else ""), flush=True)
    if not ok: fails.append(name)

db = sqlite3.connect(DB)
check("1 integrity_check", db.execute("PRAGMA integrity_check").fetchone()[0] == "ok")

idx = json.loads(gzip.decompress((ROOT / "filings_index.json.gz").read_bytes()))
rows = dict(db.execute("select accession, filing_date from filings"))
missing = [a for a in idx if a not in rows]; extra = [a for a in rows if a not in idx]
check("2 filings == index", not missing and not extra, f"index={len(idx)} filings={len(rows)} missing={len(missing)} extra={len(extra)}")

n_lines = 0; docs_per = collections.Counter()
for p in glob.glob(str(ROOT / "data" / "*" / "*.jsonl.gz")):
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        for line in fh:
            n_lines += 1; docs_per[json.loads(line)["accession"]] += 1
n_docs = db.execute("select count(*) from documents").fetchone()[0]
check("3 documents == day-file lines", n_docs == n_lines, f"db={n_docs} files={n_lines}")

bad = 0
for a, n in db.execute("select accession, count(*) from documents group by accession"):
    if docs_per.get(a) != n: bad += 1
ndocs_mismatch = db.execute("select count(*) from filings f where n_docs != (select count(*) from documents d where d.accession=f.accession)").fetchone()[0]
check("4 per-filing doc counts", bad == 0 and ndocs_mismatch == 0, f"mismatch_vs_files={bad} mismatch_vs_n_docs={ndocs_mismatch}")

mism = 0; total = 0
for sha, text in db.execute("select sha256, text from documents"):
    total += 1
    if hashlib.sha256((text or "").encode("utf-8")).hexdigest() != sha: mism += 1
check("5 text fidelity (all rows)", mism == 0, f"checked={total} mismatched={mism}")

n_items = db.execute("select count(*) from items").fetchone()[0]
n_codes = sum(len(v[2].split(",")) if v[2] else 0 for v in idx.values())
no_name = db.execute("select count(*) from items where item_text is null").fetchone()[0]
no_items = db.execute("select count(*) from filings f where items_index != '[]' and not exists (select 1 from items i where i.accession=f.accession)").fetchone()[0]
bad_code = db.execute("select count(*) from items where item_code not glob '[0-9].[0-9][0-9]'").fetchone()[0]
check("6 items == index codes", n_items == n_codes and no_items == 0 and bad_code == 0, f"items={n_items} index_codes={n_codes} filings_without_items={no_items} bad_codes={bad_code} unlabeled={no_name} (label gaps are a warning, not a failure)")

bad_dates = sum(1 for a, d in rows.items() if not d or not ("2022-07-18" <= d <= "2026-08-07") or d != idx[a][3])
check("7 dates", bad_dates == 0, f"bad={bad_dates}")

bym_idx = collections.Counter(v[3][:7] for v in idx.values()); bym_db = collections.Counter(d[:7] for d in rows.values())
check("8 per-month counts", bym_idx == bym_db, f"months={len(bym_idx)} diffs={[m for m in bym_idx if bym_idx[m]!=bym_db.get(m)]}")

print("RESULT:", "ALL CHECKS PASSED" if not fails else "FAILED: " + ", ".join(fails), flush=True)
sys.exit(0 if not fails else 1)
