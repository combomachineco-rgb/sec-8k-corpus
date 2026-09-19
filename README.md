# sec-8k-corpus

Every SEC Form 8-K (and 8-K/A) filed **2022-07-18 to 2026-08-07**, as text.
281,935 filings. Public SEC data only; nothing else lives here.

## What is stored

One row per **document** in `data/{YYYY}/{YYYY-MM-DD}.jsonl.gz` (gzipped
JSON lines, keyed by EDGAR filing date). A filing's rows share its
`accession`. Every text document of the filing is kept whole: the 8-K
body and every exhibit (`.htm`, `.html`, `.txt`), tags stripped, entities
decoded, tables kept, no length cap. Binary documents (graphics, PDF,
XBRL, zip) are not stored but are listed in `docs_skipped`.

| field | meaning |
|---|---|
| `accession`, `cik`, `company_name` | EDGAR identity |
| `form_type` | `8-K`, `8-K/A`, ... from the submission header |
| `filing_date` | EDGAR `filingDate` (nightly submissions snapshot) |
| `acceptance_datetime` | `YYYYMMDDhhmmss` ET from the submission header |
| `period_of_report` | from the header |
| `items_declared` | `ITEM INFORMATION` lines from the header |
| `items_index` | item codes from the submissions snapshot |
| `primary_document` | EDGAR's primaryDocument for the filing |
| `seq`, `doc_type`, `filename`, `description` | this document |
| `n_docs`, `docs_skipped` | how many text docs stored, which binaries were not |
| `chars`, `sha256`, `text` | the document text |
| `source_url`, `fetched_utc`, `fetcher_revision`, `submission_bytes` | provenance |

`data/{YYYY}/{YYYY-MM}.done.txt` lists finished accessions (resume index).
`data/{YYYY}/{YYYY-MM}.failed.jsonl` lists fetch failures; a re-run retries them.

## How it was fetched

`fetch_8k.py`, run by `.github/workflows/fetch.yml` on GitHub-hosted runners.
One request per filing: EDGAR's complete submission text file. Declared
User-Agent, at most 6.7 requests/second per runner, 429/503 honoured with
Retry-After. `filings_index.json.gz` is the accession list, built from
EDGAR's nightly `submissions.zip`.

## Reading it

```python
import gzip, json, glob
for path in sorted(glob.glob("data/*/*.jsonl.gz")):
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            row = json.loads(line)
```
