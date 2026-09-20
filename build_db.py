"""Build corpus.db (SQLite) from data/*/*.jsonl.gz: one queryable table set.

  filings   one row per filing (281,935): identity, dates, declared items, provenance
  documents one row per stored document (~912k): type, filename, full text
  items     one row per (filing, declared item) from the EDGAR header
Indexes on the columns downstream selects by. Text is stored uncompressed so
SQL can search it. Sections by item caption are NOT cut here; that rule lives
with the consumer. Re-runnable: rebuilds from scratch into a temp file, then
swaps it in.
"""
import glob, gzip, json, os, sqlite3, sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "corpus.db"
TMP = ROOT / "corpus.db.building"

SCHEMA = """
CREATE TABLE filings(
  accession TEXT PRIMARY KEY, cik TEXT, company_name TEXT, form_type TEXT,
  filing_date TEXT, acceptance_datetime TEXT, period_of_report TEXT,
  items_declared TEXT, items_index TEXT, primary_document TEXT, n_docs INTEGER,
  docs_skipped TEXT, submission_bytes INTEGER, source_url TEXT, fetched_utc TEXT, fetcher_revision TEXT);
CREATE TABLE documents(
  id INTEGER PRIMARY KEY, accession TEXT NOT NULL, seq INTEGER, doc_type TEXT,
  filename TEXT, description TEXT, is_xbrl_render INTEGER, chars INTEGER, sha256 TEXT, text TEXT);
CREATE TABLE items(accession TEXT NOT NULL, item_code TEXT NOT NULL, item_text TEXT);
"""
INDEXES = """
CREATE INDEX ix_filings_cik ON filings(cik);
CREATE INDEX ix_filings_date ON filings(filing_date);
CREATE INDEX ix_filings_form ON filings(form_type);
CREATE INDEX ix_docs_acc ON documents(accession);
CREATE INDEX ix_docs_type ON documents(doc_type);
CREATE INDEX ix_docs_render ON documents(is_xbrl_render);
CREATE INDEX ix_items_code ON items(item_code);
CREATE INDEX ix_items_acc ON items(accession);
"""
# EDGAR header gives item names ("Results of Operations and Financial Condition");
# the submissions index gives codes ("2.02"). Map names to codes for the items table.
ITEM_CODES = {
 "Entry into a Material Definitive Agreement":"1.01","Termination of a Material Definitive Agreement":"1.02",
 "Bankruptcy or Receivership":"1.03","Mine Safety - Reporting of Shutdowns and Patterns of Violations":"1.04",
 "Material Cybersecurity Incidents":"1.05","Completion of Acquisition or Disposition of Assets":"2.01",
 "Results of Operations and Financial Condition":"2.02",
 "Creation of a Direct Financial Obligation or an Obligation under an Off-Balance Sheet Arrangement of a Registrant":"2.03",
 "Triggering Events That Accelerate or Increase a Direct Financial Obligation or an Obligation under an Off-Balance Sheet Arrangement":"2.04",
 "Costs Associated with Exit or Disposal Activities":"2.05","Material Impairments":"2.06",
 "Notice of Delisting or Failure to Satisfy a Continued Listing Rule or Standard; Transfer of Listing":"3.01",
 "Unregistered Sales of Equity Securities":"3.02","Material Modification to Rights of Security Holders":"3.03",
 "Changes in Registrant's Certifying Accountant":"4.01",
 "Non-Reliance on Previously Issued Financial Statements or a Related Audit Report or Completed Interim Review":"4.02",
 "Changes in Control of Registrant":"5.01",
 "Departure of Directors or Certain Officers; Election of Directors; Appointment of Certain Officers: Compensatory Arrangements of Certain Officers":"5.02",
 "Amendments to Articles of Incorporation or Bylaws; Change in Fiscal Year":"5.03",
 "Temporary Suspension of Trading Under Registrant's Employee Benefit Plans":"5.04",
 "Amendments to the Registrant's Code of Ethics, or Waiver of a Provision of the Code of Ethics":"5.05",
 "Change in Shell Company Status":"5.06","Submission of Matters to a Vote of Security Holders":"5.07",
 "Shareholder Director Nominations":"5.08","Regulation FD Disclosure":"7.01","Other Events":"8.01",
 "Financial Statements and Exhibits":"9.01","ABS Informational and Computational Material":"6.01",
 "Change of Servicer or Trustee":"6.02","Change in Credit Enhancement or Other External Support":"6.03",
 "Failure to Make a Required Distribution":"6.04","Securities Act Updating Disclosure":"6.05",
 "Static Pool":"6.06","Alternative Filings of Asset-Backed Issuers":"6.10",
}

def main():
    if TMP.exists(): TMP.unlink()
    db = sqlite3.connect(TMP)
    db.executescript("PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA cache_size=-512000; PRAGMA temp_store=MEMORY;")
    db.executescript(SCHEMA)
    files = sorted(glob.glob(str(ROOT / "data" / "*" / "*.jsonl.gz")))
    t0 = time.time(); seen = set(); nf = nd = ni = 0; fbuf = []; dbuf = []; ibuf = []
    def flush():
        nonlocal fbuf, dbuf, ibuf
        db.executemany("INSERT OR IGNORE INTO filings VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", fbuf)
        db.executemany("INSERT INTO documents(accession,seq,doc_type,filename,description,is_xbrl_render,chars,sha256,text) VALUES (?,?,?,?,?,?,?,?,?)", dbuf)
        db.executemany("INSERT INTO items VALUES (?,?,?)", ibuf)
        fbuf, dbuf, ibuf = [], [], []
    for k, p in enumerate(files, 1):
        with gzip.open(p, "rt", encoding="utf-8") as fh:
            for line in fh:
                r = json.loads(line); a = r["accession"]
                if a not in seen:
                    seen.add(a); nf += 1
                    declared = r.get("items_declared") or []
                    fbuf.append((a, r.get("cik"), r.get("company_name"), r.get("form_type"), r.get("filing_date"),
                                 r.get("acceptance_datetime"), r.get("period_of_report"), json.dumps(declared),
                                 json.dumps(r.get("items_index") or []), r.get("primary_document"), r.get("n_docs"),
                                 json.dumps(r.get("docs_skipped") or []), r.get("submission_bytes"), r.get("source_url"),
                                 r.get("fetched_utc"), r.get("fetcher_revision")))
                    codes = set(r.get("items_index") or [])
                    for name in declared:
                        code = ITEM_CODES.get(name); ibuf.append((a, code or "?", name)); ni += 1
                        if code: codes.discard(code)
                    for code in sorted(codes):      # in the index but not in the header text
                        ibuf.append((a, code, None)); ni += 1
                fn = (r.get("filename") or ""); dt = r.get("doc_type") or ""
                render = 1 if (dt == "XML" and fn.startswith("R") and fn.endswith(".htm")) else 0
                dbuf.append((a, r.get("seq"), dt, fn, r.get("description"), render, r.get("chars"), r.get("sha256"), r.get("text")))
                nd += 1
                if len(dbuf) >= 2000: flush()
        if k % 100 == 0:
            flush(); db.commit()
            print(f"{k}/{len(files)} files  filings={nf} docs={nd} items={ni}  {time.time()-t0:.0f}s", flush=True)
    flush(); db.commit()
    print("indexing...", flush=True); db.executescript(INDEXES); db.commit()
    db.execute("PRAGMA journal_mode=WAL"); db.close()
    os.replace(TMP, OUT)
    print(f"DONE {OUT} filings={nf} docs={nd} items={ni} size={OUT.stat().st_size/1e9:.1f} GB in {(time.time()-t0)/60:.1f} min", flush=True)
    return 0

if __name__ == "__main__":
    sys.exit(main())
