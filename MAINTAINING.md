# Maintaining UMDB

Operational guide for whoever keeps the database running: workflows, secrets, releases, and what to do when something breaks. UMDB serves its own browser of the live catalog (`docs/`, GitHub Pages). The release browser ([UMA](https://github.com/aglucaci/UMA)) and the analysis pipeline ([STRIGA](https://github.com/aglucaci/STRIGA)) have their own repositories; this guide covers how UMDB hands data to them. For setting up a development environment, see [CONTRIBUTING.md](CONTRIBUTING.md).

## How a data update flows

```
umdb catalog pull (restore data/ from the catalog-latest release)
  → NCBI esearch (7 query profiles in parallel, a month per search)
  → esummary (titles, study names; 200 per request)
  → efetch RunInfo (200 experiments per request)
  → BioSample / BioProject enrichment (batched; cached in data/cache/)
  → rule-based geo + assay + eligibility annotation
  → GeoNames validation of every location (data/reference/geonames, offline)
  → optional AI review (cached in data/cache/ai_curation.json)
  → append to data/srr_catalog_<YEAR>.jsonl
  → rebuild data/umdb.sqlite, then docs/db/ (the live-catalog website)
  → commit docs/ and reports/ → GitHub Pages publishes aglucaci.github.io/UMDB
  → umdb catalog push (upload data/ to catalog-latest)

and, when you decide to release:

  → STRIGA on data/  →  umdb release --version V --analysis results/
  → GitHub Release v<V> (+ Zenodo DOI)  →  UMA rebuilds from it
```

Harvests change the catalog, and UMDB's own site, continuously; UMA changes only when a versioned release is published, so what people cite is always a release.

## Scheduled and manual workflows

| Workflow | Trigger | What it does |
| --- | --- | --- |
| [`umdb_daily_crawl.yml`](.github/workflows/umdb_daily_crawl.yml) | Daily 13:00 UTC; manual | Small date-sorted crawl (stops after 25 new runs), pushes the catalog, rebuilds `data/umdb.sqlite` and `docs/db`, commits `docs/` and `reports/` |
| [`umdb_full_ai_curation.yml`](.github/workflows/umdb_full_ai_curation.yml) | Manual only | AI review of the whole catalog or one year, pushes the catalog, rebuilds the database and `docs/db`, commits `docs/` and `reports/` |

Manual runs of the daily crawl can choose `crawl`, `daily` (recent N days) or `backfill-year`, along with query profiles, page size and optional AI curation.


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

**Speed.** Each search covers a calendar month (with no per-day cap) and all profiles are searched at once. Records are fetched in bulk: RunInfo and BioSamples 200 per request, several requests in parallel, under one shared limit of 8 requests/s with an API key (2.8 without), slowed automatically if NCBI answers 429. In testing, a day of 1,268 experiments took 17 s instead of 11.5 min. Tune with `UMDB_NCBI_RPS` and `UMDB_NCBI_WORKERS` in `.env`; lower `UMDB_NCBI_RPS` if two harvests share one key, since NCBI's limit is per key.

Other options: `--max-per-day N` (cap instead of paging everything), `--profile NAME`, `--oldest-first`, `--ai`, `--redo`. Run `umdb backfill --help` for the list.

Progress streams to the terminal: per-window hits, timings for the RunInfo, BioSample and BioProject stages, runs written, an ETA for the current year and for the remaining years. Add `-v` for every NCBI request, or `-q` for year summaries only. Logs and markers are in `logs/backfill/` (git-ignored; logs are plain text): `<year>.done` for normal runs, `<year>.refresh.done` for refresh runs. Delete a marker, or pass `--redo`, to run a year again. A refresh re-requests everything already held; with batching a year of about 60,000 experiments needs on the order of a thousand requests.

Exports always publish only the newest record per run, so the site is correct even before compaction.

In GitHub: Actions → **UMDB Daily Crawl** → Run workflow → `crawl_mode: backfill-year`, `year: 2024`. The six-hour job limit makes local runs more practical for deep backfills.

Date searches use SRA's `pdat` (public release date). SRA ignores `edat`, and before 2026-11 that made backfill and daily runs return nothing.

### Re-annotate the catalog after a rule change

Changes to `assay.py` or `biosample.py` apply to the **whole** catalog on the next database build (`umdb db build`), because `exports.refresh_derived_annotations()` re-derives geo and assay from cached metadata. No re-harvest is needed: `umdb export` rebuilds the database and the live site; the change reaches UMA with the next release.

Records with no cached BioSample keep the geo values they were harvested with.

### Location validation

Locations are validated against GeoNames at every database build (`geo.py`). The extract (countryInfo, admin1 codes, cities500; about 14 MB) lives in `data/reference/geonames/` and travels with the catalog bundle, so workflows rebuild with the same gazetteer. To refresh it, run `umdb geo fetch --force`, then `umdb export`; the download date and checksum are recorded in the database `meta` table and `docs/db/manifest.json`.

To see why a location string resolved the way it did:

```bash
umdb geo check "USA: Portland, ME"
umdb geo check "China:Beijing" --lat 39.54 --lon 116.24
```

Rule changes go in `geo.py`; bump `GEO_RULES_VERSION` and add a test case to `GazetteerTests`. Review what changed with `umdb db query "SELECT status, count(*) FROM sample_locations GROUP BY 1"`, and look at `ambiguous` and `conflicting` rows, which are the curation queue.

### Change the discovery queries

Edit `QUERY_PROFILES` in `config.py`, bump `QUERY_PROFILES_VERSION`, and say why in the comment. The version is written to `reports/debug/latest_report.json` with every run, so each release can be traced to the queries that produced it.

Already-seen runs are skipped (`data/seen_srr_runs.txt`), so newly in-scope studies are only picked up by a crawl or backfill that reaches them.

### Rotate a secret

Update the repository secret and your local `.env`. If a key was exposed, follow [SECURITY.md](SECURITY.md).

## The catalog lives in Release assets

The harvested catalog (`data/srr_catalog_*.jsonl`, `data/cache/*.json`, `data/seen_*.txt`) is git-ignored. It grows by hundreds of MB per deep backfill and changes on every run, which would make the repository grow without bound. It is stored instead as one compressed bundle (`umdb-catalog.tar.gz`, about 18 times smaller; 1.2 GB packs to 65 MB) with a SHA-256 manifest, in the GitHub release `catalog-latest`.

```bash
umdb catalog status            # local files, last pull/push, what is published
umdb catalog pull              # restore data/ from catalog-latest (verifies checksums)
umdb deepest --commit          # harvest; commits docs/ and reports/ (site data and run reports)
umdb catalog push              # upload data/ to catalog-latest
umdb catalog push --snapshot   # also keep a dated catalog-YYYY.MM.DD release
```

Both workflows pull the catalog before harvesting and push it afterwards; if `catalog-latest` is missing they stop before harvesting, so a run can never publish from an empty `data/`. A push is refused when the published bundle is not the one this checkout last pulled or pushed (someone else harvested meanwhile): run `umdb catalog pull --merge`, which keeps local records and adds the published ones (the newest record per run still wins; `umdb compact` drops superseded rows), then push. `pull` refuses to replace local changes unless you pass `--merge` or `--force`. Pushing needs `gh` authenticated with write access to the repository.

**First upload.** After the deep backfill finishes: `umdb compact`, `umdb catalog push --snapshot`, then remove the catalog from the index with `git rm -r --cached data/srr_catalog_*.jsonl data/cache/*.json data/seen_*.txt` and commit. Older commits still contain catalog versions, so to shrink the repository itself, rewrite history before pushing (see the notes in the changelog).

## State files and what not to delete

| File | Purpose | If deleted |
| --- | --- | --- |
| `data/seen_sra_uids.txt`, `data/seen_srr_runs.txt` | De-duplication across runs | Next crawl re-ingests everything (exports still publish one record per run; `umdb compact` removes the extra rows) |
| `data/srr_catalog_*.jsonl` (and `_partNNN`) | The canonical run catalog | **Data loss.** Recover from git history |
| `data/cache/biosample.json`, `bioproject*.json` | NCBI response cache; also input to offline re-annotation | Re-fetched on demand (slow); offline re-annotation degrades |
| `data/cache/ai_curation.json` | AI review results | AI reviews must be re-run (costs money) |
| `reports/*` | Latest run reports (committed by the workflows) | Rewritten by the next harvest |
| `docs/db/*` | Live-catalog website data | Regenerated by `umdb export` from `data/umdb.sqlite` |
| `catalog-latest` release | Off-machine copy of everything in `data/` above | Push again from a checkout that has the catalog |
| `data/umdb.sqlite` | Query database (git-ignored) | Rebuilt by `umdb db build` (a few minutes) |
| `data/reference/geonames/` | GeoNames extract used to validate locations (git-ignored, in the catalog bundle) | Re-downloaded automatically on the next build (`umdb geo fetch`); a newer GeoNames extract can change a few place assignments |

Seen runs are skipped by default, so corrections NCBI submitters make to already-ingested records are only picked up by a refresh (`umdb deepest`, or `--refresh` on `crawl`, `daily` and `backfill-year`). `umdb compact` then drops the superseded rows.

## Size limits

- GitHub rejects files over 100 MB and warns over 50 MB; Pages sites should stay under 1 GB. The catalog is not in git (Release assets allow 2 GB per file). Catalog files are still split into 50 MB `_partNNN.jsonl` parts.
- The live site loads about 1 MB compressed at start; per-project files (`docs/db/p/`) total about 260 MB for 477,000 runs. UMA holds a similar amount per release.
- Older history contains catalog and run-record files. Use `git clone --depth 1` when history is not needed.

## Troubleshooting

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| Backfill finishes instantly with 0 new runs | Code older than 2026-11 (`edat` date search) | Update; see the CHANGELOG |
| Workflow green, nothing new committed | The `.cli` invocation (see Known issue), or genuinely no new runs | Check the `[SUMMARY]` lines in the job log; no lines means nothing ran |
| `CERTIFICATE_VERIFY_FAILED` / `self-signed certificate in certificate chain` | A network or security tool inspects HTTPS and presents its own root, which Python's CA bundle does not trust | On macOS UMDB retries automatically with the System keychain roots (still verified). If that fails, export your organisation's root as PEM and set `UMDB_CA_BUNDLE=/path/root.pem` in `.env`, or run off the inspecting network. Verification is never disabled |
| `HTTP failed: https://eutils...` | NCBI rate limiting or an outage | Check that the API key is set; retry later. `http_get` already retries 6 times with backoff |
| `OPENAI_API_KEY is required` | AI curation requested without a key | Add the secret or `.env` entry, or drop `--ai-curate` |
| Push rejected: file too large | A part file exceeded 100 MB | Lower `MAX_OUTPUT_BYTES`, rebuild, amend the commit |
| UMDB site shows old numbers | Pages build lag, or the browser cache | Wait a few minutes; hard-refresh |
| UMA shows old numbers | UMA shows the latest *release*, not the latest harvest | Make a release (below) and rebuild UMA |
| Push conflict in a workflow | Two workflows committed at once | Re-run; the daily crawl uses a concurrency group |

Add `-v` to any `umdb` command to see each NCBI request (API key redacted) and cache hit as it happens. Per-run debug files: add `--debug` to write `data/debug/report_<tag>.json` and decision logs (not committed).

## Releases

A release is the catalog at one moment plus STRIGA's analyses of exactly that catalog; UMA is rebuilt from it. Layout and the contract between the repositories: [ARCHITECTURE.md](ARCHITECTURE.md#releases).

```bash
pip install -r requirements-release.txt          # once: pyarrow (+ duckdb)
umdb catalog pull && umdb db build               # the catalog to release; do not harvest until released

# 1. Analyse it (STRIGA hashes data/umdb.sqlite; pin the STRIGA version you cite)
nextflow run aglucaci/STRIGA -r v0.2.0 -profile conda --catalog data --fetch_worldbank --outdir results

# 2. Write the release; refuses STRIGA results from a different database
umdb release --version 1.0.0 --analysis results  # dist/releases/1.0.0/ and dist/releases/umdb-1.0.0.tar.gz
(cd dist/releases/1.0.0 && shasum -a 256 -c checksums.sha256)

# 3. Publish it
git tag v1.0.0 && git push origin v1.0.0
gh release create v1.0.0 dist/releases/umdb-1.0.0.tar.gz --title "UMDB 1.0.0" --notes-file CHANGELOG.md
```

4. For a DOI, deposit `umdb-1.0.0.tar.gz` in Zenodo; `.zenodo.json` holds the title, description, creators, licence, keywords and links to STRIGA and UMA. Put the DOI in [CITATION.cff](CITATION.cff).
5. Rebuild the website: in the UMA repository run the **Build site from a UMDB release** workflow with version `1.0.0` (or locally `uma fetch 1.0.0 && uma build 1.0.0`, then commit `docs/`). UMA reads the DOI from the release manifest if you add it there before uploading, or from the workflow's `doi` input.
6. Record the version, `QUERY_PROFILES_VERSION`, eligibility `RULES_VERSION`, the STRIGA version and the counts (all in `manifest.json`) in [CHANGELOG.md](CHANGELOG.md).

A release is about 230 MB, mostly the compressed SQLite database, so it fits one GitHub Release asset (2 GB limit). Object storage with HTTP range requests (for example Cloudflare R2) becomes useful once browsers query the Parquet files directly.

`dist/` is git-ignored; releases are never committed.
