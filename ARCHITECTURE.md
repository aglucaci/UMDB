# UMDB data architecture

How UMDB stores, queries and publishes its data, and why. Field definitions are in [DATA_DICTIONARY.md](DATA_DICTIONARY.md). Day-to-day operation is in [MAINTAINING.md](MAINTAINING.md).

```
NCBI SRA · BioSample · BioProject
        │  umdb crawl / daily / backfill-year / deepest
        ▼
data/srr_catalog_*.jsonl  +  data/cache/*.json        ← source of truth, in git
  (slim run records, append-only;                       (ingestion log + NCBI caches)
   newest record per run wins)
        │  umdb db build        (automatic after every harvest)
        ▼
data/umdb.sqlite                                       ← query database, derived, git-ignored
  normalized tables · indexes · FTS5 search · views
        │                               │
        │  umdb release                 │  umdb export
        ▼                               ▼
dist/releases/<version>/               docs/db/*.json
  Parquet + TSV + schema +               website data (GitHub Pages)
  manifest + checksums
  → GitHub Releases / Zenodo (DOI)
```

## Which format does what

| Layer | Format | Location | Role |
| --- | --- | --- | --- |
| Source of truth | JSON Lines + JSON caches | `data/` (git) | Append-only ingestion log of slim run records, plus raw BioSample/BioProject responses. Reviewable diffs, full history, works in GitHub Actions without extra storage |
| Query database | SQLite (`umdb.sqlite`) | `data/` (git-ignored) | Normalized, indexed, full-text searchable. Rebuilt in about 30 s, deterministically, with the current geography, assay and eligibility rules |
| Research releases | Parquet (Zstandard) + gzipped TSV | `dist/releases/<version>/` (not in git) | Compact, typed, versioned snapshots with checksums, for DuckDB, R, Python or spreadsheets. About 11 MB for the whole catalog |
| Website | JSON | `docs/db/` (git, GitHub Pages) | What the explorer loads today |

**Why SQLite is derived, not stored.** A binary file that changes on every harvest makes a poor git object: each change adds the whole file to history, and git cannot diff or merge it. Keeping the slim JSONL as the record means every change is reviewable and the database can always be rebuilt identically. If the catalog outgrows git, the JSONL log can move to Release assets without changing the database or release code.

## SQLite schema (version 1.0.0)

BioSample is the principal unit of observation: one sample can have several experiments and runs, and location, environment and urban eligibility belong to the sample.

| Table | Key | Contents |
| --- | --- | --- |
| `projects` | BioProject accession | Title, description, organism, data type, center, dates, raw esummary JSON |
| `samples` | BioSample accession | Collection date (raw, ISO, year), location (raw, country, region, city, coordinates, resolution, validation), urban eligibility tier, context, reason, evidence, environment fields, all submitted attributes |
| `experiments` | SRX/ERX/DRX | Study, project, library strategy/source/selection/layout, platform, instrument, assay class |
| `runs` | SRR/ERR/DRR | Experiment, sample, project, release date/year, spots, bases, read length, size, organism label, center, download URL, run-level eligibility, raw RunInfo JSON |
| `project_samples` | (project, sample) | Link table |
| `ai_annotations` | Run accession | AI-proposed annotations, kept separate from rule output (`status = proposed`) |
| `search` | FTS5 | Full-text index over project titles and descriptions, sample titles, environment fields and location strings |
| `meta` | key | Schema, rules and query-profile versions, build time, row counts |

Views: `umdb_samples` (one row per BioSample with projects, context, location, dates, run count and assay classes) and `umdb_runs` (one row per run joined to its sample, experiment and project).

**Sample tier.** A sample takes the most urban tier among its runs. Runs of one sample share the BioSample evidence; they differ only through run-level labels such as the organism, so the strongest evidence wins. Run-level tiers stay in `runs.urban_status`.

**Unknown is NULL.** Empty strings and placeholders ("missing", "not collected", …) become NULL. `collection_date` is filled only when a full day is given; `collection_year` keeps partial dates.

**Indexes:** tier, context, country, city, collection date and year; a composite cohort index `(urban_status, urban_context, country, collection_date)`; and run, experiment and project foreign keys. On the current catalog, typical cohort and full-text queries take 1–11 ms.

## Using the database

```bash
umdb db build                       # rebuild (also runs after every harvest)
umdb db info                        # versions, counts, samples per tier
umdb db query "SELECT urban_context, count(*) FROM samples WHERE urban_status='confirmed_urban' GROUP BY 1"
umdb db shell                       # interactive sqlite3
```

Example queries:

```sql
-- Confirmed-urban municipal wastewater samples collected 2020–2025
SELECT biosample_accession, country, city, collection_date
FROM umdb_samples
WHERE urban_status = 'confirmed_urban' AND urban_context = 'municipal_wastewater'
  AND collection_date >= '2020-01-01' AND collection_date < '2026-01-01';

-- Full-text search across projects and samples
SELECT kind, accession, title FROM search WHERE search MATCH 'subway OR metro' ORDER BY rank LIMIT 20;

-- Download URLs for every run of confirmed transit samples
SELECT r.accession, r.download_url
FROM runs r JOIN samples s ON s.accession = r.sample_accession
WHERE s.urban_status = 'confirmed_urban' AND s.urban_context = 'transit';
```

## Releases

```bash
pip install -r requirements-release.txt     # pyarrow (+ duckdb for analysis); once
umdb release --version 1.0.0                # or omit --version for today's date
```

This writes `dist/releases/1.0.0/`:

| File | Contents |
| --- | --- |
| `projects`, `samples`, `experiments`, `runs`, `project_samples`, `ai_annotations` `.parquet` | Normalized tables (verbatim RunInfo and raw esummary JSON omitted; they stay in SQLite and git) |
| `umdb_samples.parquet`, `umdb_runs.parquet` | Denormalized convenience tables |
| `umdb_samples.tsv.gz`, `umdb_runs.tsv.gz` | The same, as tab-separated text |
| `schema.json` | Columns and types of every file |
| `manifest.json` | Version, build time, schema, query-profile and rules versions, licence, row counts, sizes, SHA-256 |
| `checksums.sha256` | Verify with `shasum -a 256 -c checksums.sha256` |

Parquet files use Zstandard compression, typed integers, floats and dates, and NULL for unknown values. Analyse them directly with DuckDB:

```sql
SELECT urban_context, count(*) AS samples
FROM read_parquet('dist/releases/1.0.0/umdb_samples.parquet')
WHERE urban_status IN ('confirmed_urban', 'probable_urban')
GROUP BY 1 ORDER BY samples DESC;
```

Publish a release by attaching the folder to a GitHub Release, or deposit it in Zenodo for a DOI and add the DOI to [CITATION.cff](CITATION.cff) (see [MAINTAINING.md](MAINTAINING.md#releases)).

## Not yet built

- **Website on compact indexes:** the explorer still downloads every run (243 MB). Planned: a small project index plus per-sample detail files loaded on demand, or DuckDB-Wasm over the Parquet release.
- **Discovery provenance:** which query profile found each experiment, and when (a `discovery_hits` table).
- **Validated locations:** ISO 3166 country codes and GeoNames IDs (a `locations` table).
- **Release history table** inside the database.
