# Maintaining UMDB

Operational guide for whoever keeps the database running: workflows, secrets, releases, and what to do when something breaks. For setting up a development environment, see [CONTRIBUTING.md](CONTRIBUTING.md).

## How a data update flows

```
NCBI esearch (5 query profiles)
  → esummary (titles, study names; batches of 200)
  → efetch runinfo (one row per run)
  → BioSample / BioProject enrichment (cached in data/cache/)
  → rule-based geo + assay annotation
  → optional AI review (cached in data/cache/ai_curation.json)
  → append to data/srr_catalog_<YEAR>.jsonl
  → rebuild docs/db/ (re-derives rule annotations, merges AI cache)
  → commit → GitHub Pages publishes docs/
```

A commit to `main` that touches `docs/` **is** a database release. The site has no backend.

## Scheduled and manual workflows

| Workflow | Trigger | What it does |
| --- | --- | --- |
| [`umdb_daily_crawl.yml`](.github/workflows/umdb_daily_crawl.yml) | Daily 13:00 UTC; manual | Small date-sorted crawl (stops after 25 new runs), rebuilds `docs/db`, commits `data/` and `docs/` |
| [`umdb_full_ai_curation.yml`](.github/workflows/umdb_full_ai_curation.yml) | Manual only | AI review of the whole catalog or one year, rebuilds exports, commits |

Manual runs of the daily crawl can choose `crawl`, `daily` (recent N days) or `backfill-year`, along with query profiles, page size and optional AI curation.

> **Known issue:** both workflows invoke `python3 -m scripts.umdb_harvester.cli`, which exits immediately without running (`cli.py` has no `__main__` block). Until they call `python3 -m scripts.umdb_harvester`, the scheduled crawl succeeds without harvesting anything. The catalog has not grown from a scheduled run since February 2026.

### Repository secrets

Set these under **Settings → Secrets and variables → Actions**:

| Secret | Required by | Notes |
| --- | --- | --- |
| `NCBI_API_KEY` | Both workflows (recommended) | Raises the NCBI rate limit from 3 to 10 requests/s |
| `NCBI_EMAIL` | Both workflows (recommended) | Sent to NCBI with every request, as their guidelines ask |
| `OPENAI_API_KEY` | AI curation | Required whenever `--ai-curate` or `curate-ai` runs |

Locally, the same names go in `.env` (see [.env.example](.env.example)).

## Routine tasks

### Backfill years (deepest search)

Locally, use the wrapper. It runs `backfill-year` for each year with the seven default profiles, paging through **every** hit for every day (no per-day cap). It logs each year and skips completed years when re-run.

**Deepest possible search, overwriting old data:**

```bash
git add data && git commit -m "Snapshot before deep refresh"   # recoverable starting point
umdb deepest                       # 2007 .. this year, newest first
umdb deepest 2018 2024 --commit    # a range, committing after each year
umdb deepest --dry-run             # show the plan
```

`umdb deepest` (= `umdb backfill --deepest`) adds two things to a normal backfill:

- **`--refresh`:** experiments and runs already in the catalog are harvested again, and their BioSample and BioProject records are re-fetched (once per accession per run), so NCBI corrections and newly added attributes are picked up. The new run record is appended and supersedes the old one.
- **`--compact`:** after each year, `data/srr_catalog_*.jsonl` is rewritten keeping only the newest record per run (latest `provenance.ingested_utc`). This permanently drops the superseded rows; earlier versions survive only in git history. If `data/` has uncommitted changes, the script asks before starting (`--yes` skips the prompt).

**New records only** (faster; skips everything already ingested):

```bash
umdb backfill                      # 2007 .. this year, newest first
umdb backfill 2018 2024 --commit
```

Other options: `--max-per-day N` (cap instead of paging everything), `--profile NAME`, `--oldest-first`, `--ai`, `--redo`. Run `umdb backfill --help` for the list.

Progress streams to the terminal: per-day hits, runs written, an ETA for the current year and for the remaining years. Add `-v` for every NCBI request, or `-q` for year summaries only. Logs and markers are in `logs/backfill/` (git-ignored; logs are plain text): `<year>.done` for normal runs, `<year>.refresh.done` for refresh runs. Delete a marker, or pass `--redo`, to run a year again. Each year makes hundreds of thousands of NCBI requests, and a refresh re-requests everything already held, so expect many hours per year even with an API key.

Exports always publish only the newest record per run, so the site is correct even before compaction.

In GitHub: Actions → **UMDB Daily Crawl** → Run workflow → `crawl_mode: backfill-year`, `year: 2024`. The six-hour job limit makes local runs more practical for deep backfills.

Date searches use SRA's `pdat` (public release date). SRA ignores `edat`, and before 2026-11 that made backfill and daily runs return nothing.

### Re-annotate the catalog after a rule change

Changes to `assay.py` or `biosample.py` apply to the **whole** catalog on the next export, because `exports.refresh_derived_annotations()` re-derives geo and assay from cached metadata. No re-harvest is needed: run any harvester command, or rebuild exports directly (see CONTRIBUTING), then commit `docs/db/`.

Records with no cached BioSample keep the geo values they were harvested with.

### Change the discovery queries

Edit `QUERY_PROFILES` in `config.py`, bump `QUERY_PROFILES_VERSION`, and say why in the comment. The version is written to `docs/debug/latest_report.json` with every run, so each release can be traced to the queries that produced it.

Already-seen runs are skipped (`data/seen_srr_runs.txt`), so newly in-scope studies are only picked up by a crawl or backfill that reaches them.

### Rotate a secret

Update the repository secret and your local `.env`. If a key was exposed, follow [SECURITY.md](SECURITY.md).

## State files and what not to delete

| File | Purpose | If deleted |
| --- | --- | --- |
| `data/seen_sra_uids.txt`, `data/seen_srr_runs.txt` | De-duplication across runs | Next crawl re-ingests everything (exports still publish one record per run; `umdb compact` removes the extra rows) |
| `data/srr_catalog_*.jsonl` (and `_partNNN`) | The canonical run catalog | **Data loss.** Recover from git history |
| `data/cache/biosample.json`, `bioproject*.json` | NCBI response cache; also input to offline re-annotation | Re-fetched on demand (slow); offline re-annotation degrades |
| `data/cache/ai_curation.json` | AI review results | AI reviews must be re-run (costs money) |
| `docs/db/*` | Published release | Regenerated by the next export |

Seen runs are skipped by default, so corrections NCBI submitters make to already-ingested records are only picked up by a refresh (`umdb deepest`, or `--refresh` on `crawl`, `daily` and `backfill-year`). `umdb compact` then drops the superseded rows.

## Size limits

- GitHub rejects files over 100 MB and warns over 50 MB. Exports are chunked at 50 MB (`MAX_OUTPUT_BYTES`); catalogs are split into `_partNNN.jsonl`.
- `docs/db/biosamples.json` (about 38 MB) is the file closest to the limit as the catalog grows.
- The repository is several hundred MB. Use `git clone --depth 1` when history is not needed.

## Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Backfill finishes instantly with 0 new runs | Code older than 2026-11 (`edat` date search) | Update; see the CHANGELOG |
| Workflow green, nothing new committed | The `.cli` invocation (see Known issue), or genuinely no new runs | Check the `[SUMMARY]` lines in the job log; no lines means nothing ran |
| `CERTIFICATE_VERIFY_FAILED` / `self-signed certificate in certificate chain` | A network or security tool inspects HTTPS and presents its own root, which Python's CA bundle does not trust | On macOS UMDB retries automatically with the System keychain roots (still verified). If that fails, export your organisation's root as PEM and set `UMDB_CA_BUNDLE=/path/root.pem` in `.env`, or run off the inspecting network. Verification is never disabled |
| `HTTP failed: https://eutils...` | NCBI rate limiting or an outage | Check that the API key is set; retry later. `http_get` already retries 6 times with backoff |
| `OPENAI_API_KEY is required` | AI curation requested without a key | Add the secret or `.env` entry, or drop `--ai-curate` |
| Push rejected: file too large | A part file exceeded 100 MB | Lower `MAX_OUTPUT_BYTES`, rebuild, amend the commit |
| Site shows old numbers | Pages build lag, or the browser cache | Wait a few minutes; hard-refresh |
| Push conflict in a workflow | Two workflows committed at once | Re-run; the daily crawl uses a concurrency group |

Add `-v` to any `umdb` command to see each NCBI request (API key redacted) and cache hit as it happens. Per-run debug files: add `--debug` to write `data/debug/report_<tag>.json` and decision logs (not committed).

## Releases

Today every commit to `docs/` is a live, unversioned release. For citable releases (planned):

1. Freeze: tag `vX.Y.Z` on a commit with a clean export.
2. Record the tag, `QUERY_PROFILES_VERSION`, catalog counts and checksums in [CHANGELOG.md](CHANGELOG.md).
3. Archive the `docs/db/` snapshot with a DOI (for example Zenodo's GitHub integration) and add the DOI to [CITATION.cff](CITATION.cff).
