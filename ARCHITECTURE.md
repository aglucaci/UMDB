# UMDB data architecture

How UMDB stores, queries and publishes its data, and why. Field definitions are in [DATA_DICTIONARY.md](DATA_DICTIONARY.md). Day-to-day operation is in [MAINTAINING.md](MAINTAINING.md).

```
NCBI SRA · BioSample · BioProject
        │  umdb crawl / daily / backfill-year / deepest
        ▼
data/srr_catalog_*.jsonl  +  data/cache/*.json        ← source of truth, git-ignored;
  (slim run records, append-only;                         stored in the GitHub release
   newest record per run wins)                            catalog-latest (umdb catalog push/pull)
        │  umdb db build        (automatic after every harvest)
        ▼
data/umdb.sqlite                                       ← query database, derived, git-ignored
  normalized tables · indexes · FTS5 search · views    ← data/reference/geonames (GeoNames, CC BY):
  sample_locations: one validated location per sample     every location validated at build time
        │                               │
        │  umdb release                 │  umdb export (automatic after every harvest)
        ▼                               ▼
dist/releases/<version>/               docs/db/  summary · geo · samples · cube · projects
  Parquet + TSV + schema +                       p/<ACC>.json per project, on demand
  manifest + checksums                 website data, in git (GitHub Pages)
  → GitHub Releases / Zenodo (DOI)
```

## Which format does what

| Layer | Format | Location | Role |
| --- | --- | --- | --- |
| Source of truth | JSON Lines + JSON caches | `data/` (git-ignored), bundled in the `catalog-latest` release | Append-only ingestion log of slim run records, plus raw BioSample/BioProject responses. One 18x-compressed bundle with checksums; pull and push from any checkout or workflow |
| Query database | SQLite (`umdb.sqlite`) | `data/` (git-ignored) | Normalized, indexed, full-text searchable, with gazetteer-validated locations. Rebuilt deterministically from the catalog and the GeoNames extract, with the current geography, assay and eligibility rules |
| Gazetteer | GeoNames text/zip files | `data/reference/geonames/` (git-ignored), bundled with the catalog | ISO countries, admin1 regions, populated places (≥ 500 people). Downloaded once (`umdb geo fetch`); checksums and download date recorded in the database `meta` and the site manifest |
| Research releases | Parquet (Zstandard) + gzipped TSV | `dist/releases/<version>/` (not in git) | Compact, typed, versioned snapshots with checksums, for DuckDB, R, Python or spreadsheets. About 11 MB for the whole catalog |
| Website | Compact JSON | `docs/db/` (git, GitHub Pages) | Aggregated index for filters, charts and the map, plus one file per project loaded on demand |

**Why SQLite is derived, not stored.** A binary file that changes on every harvest makes a poor git object: each change adds the whole file to history, and git cannot diff or merge it. Keeping the slim JSONL as the record means the database can always be rebuilt identically.

**Why the catalog is in Release assets, not git.** After the 2026 deep backfill the JSONL catalog passed 1 GB, and every harvest rewrites large parts of it, so each commit added hundreds of MB to history. As one gzipped bundle it is about 18 times smaller, and replacing a release asset adds nothing to the repository. `umdb catalog push` refuses to overwrite a bundle uploaded by someone else since your last pull; `pull --merge` combines them (append-only log, newest record per run wins).

## Website data

The site loads `summary.json`, `geo.json`, `samples.json`, `cube.json` and `projects.json` (about 5 MB uncompressed for 477,000 runs) and fetches `p/<ACC>.json` when a project is opened or exported. Runs and BioSamples are aggregated separately so that both are counted exactly:

- `samples.json` has one row contribution per BioSample. A sample's projects and assays are encoded as sets, so a sample with runs of two assays, or shared by two studies, is still counted once (26,785 rows for 406,177 BioSamples).
- `cube.json` aggregates runs by the same dimensions (23,797 rows for 477,166 runs).
- `geo.json` is the validated location hierarchy both cubes index into.

`docs/assets/db.js` turns these into a query engine shared by the Explorer and the Map. A single pass over each cube computes the totals and, for every filter, the counts it would have with all other filters applied, so cascading country → region → city lists only offer combinations that exist. Filters live in the page address (`?country=US&region=US.NY&city_id=5128581&cyear=2018-2020`), so a selection on one page opens identically on the other. A test checks the cube totals against `COUNT(DISTINCT …)` in SQLite. Built by `site.py`; unchanged files are left untouched. Field reference: [DATA_DICTIONARY.md](DATA_DICTIONARY.md#published-files-docs).

### v1.0 platform pages

The site is static (no build step, no server). Every page reads the files above through `docs/assets/db.js`:

| Page | Route | What it reads |
| --- | --- | --- |
| Explorer, Atlas | `explorer.html`, `global-map.html` | the cubes (`samples.json`, `cube.json`), `geo.json`, `population.json` (per-capita layer) |
| Study, BioSample, run | `study.html?acc=`, `sample.html?acc=`, `run.html?acc=` | `p/<ACC>.json`; BioSamples and runs are found through `ix/<NN>.json` |
| City, country, habitat | `city.html?id=`, `country.html?iso=`, `habitat.html?id=` | the cubes, filtered like the Explorer |
| Collections | `collections.html[?slug=]` | `collections.json`, `collections/<slug>.tsv.gz` |
| Workspace | `workspace.html` | the cubes (exact set operations over sample-cube rows) and project files (export) |
| Analytics | `analytics.html` | `cube.json`, `summary.json`, `analytics.json` |

`404.html` maps the routes `/study/<acc>`, `/sample/<acc>`, `/run/<acc>`, `/city/<id>`, `/country/<iso>`, `/habitat/<slug>` and `/collection/<slug>` to these pages on GitHub Pages. `assets/ui.js` holds shared labels, charts (each downloadable as CSV, and SVG charts as SVG or PNG) and Workspace storage (`localStorage`, key `umdb.workspace.v1`).

Because each BioSample is in exactly one sample-cube row, a cohort is a 0/1 mask over rows, and union, intersection and difference of cohorts are exact. Exporting a cohort loads the project files it touches and applies the same filters per sample (`sampleMatchesFull`); `scripts/umdb_harvester/cohorts.py` implements the same filters in SQL, and the same seeded balancing, so `umdb cohort export cohort.json` gives the same samples as the browser.

## Location validation

Raw `geo_loc_name` text is free-form ("USA: New York, NY", "China:Hanzhou", "R10016"). `geo.py` resolves every sample at build time, offline, against GeoNames (cities500, admin1 codes, countryInfo): country → ISO 3166-1, region → admin1 code, place → GeoNames id, checked against reported coordinates. Each distinct location string is resolved once (about 7 s for 406,000 samples). The output is one row per sample in `sample_locations` with a status (verified, probable, ambiguous, conflicting, unresolved), the method and the evidence; see [DATA_DICTIONARY.md](DATA_DICTIONARY.md#validated-locations). Maps, filters, summaries and exports use only this table. Rules worth knowing:

- A place is accepted only when the text names one place in the country (after any region constraint), or one of several same-named places is the capital and most populous, or five times larger than the next; otherwise the sample stays at country level.
- Precise coordinates confirm a place within 30 km (wider for large cities) and contradict it beyond; coordinates given to whole degrees are too coarse to do either.
- Coordinates alone assign a place (probable) only if the point lies within that settlement's extent, scaled by population (about 2.5 km for a village, up to 25 km for a large city), and never an administrative unit such as a county. A sample 8 km from a village keeps its country and no city.
- A place always lies in the sample's country; the database rejects anything else.

## SQLite schema (version 1.1.0)

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
| `countries` | ISO alpha-2 | ISO 3166-1 codes (alpha-3, numeric), name, continent |
| `admin1` | GeoNames admin1 code | First-order region name and country |
| `places` | GeoNames id | Populated places referenced by samples: name, country, region, coordinates, population, feature code |
| `sample_locations` | BioSample accession | The accepted location: country and its status, region, place, place status, method, distance to reported coordinates, coordinate precision, matched text, note, rules version |
| `meta` | key | Schema, rules, location-rules and query-profile versions, GeoNames download date and checksum, build time, row counts |

Views: `umdb_samples` (one row per BioSample with projects, context, raw and validated location, dates, run count and assay classes) and `umdb_runs` (one row per run joined to its sample, experiment and project).

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

This writes `dist/releases/1.0.0/` (normalized tables under `parquet/`, plus `studies`, `locations`, `curation_evidence` and `environmental_metadata` layers, `tsv/`, `indexes/`, `collections/`, `documentation/`, the compressed database, `.zenodo.json`; the table below lists the core files):

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
FROM read_parquet('dist/releases/1.0.0/parquet/umdb_samples.parquet')
WHERE urban_status IN ('confirmed_urban', 'probable_urban')
GROUP BY 1 ORDER BY samples DESC;
```

Publish a release by attaching the folder to a GitHub Release, or deposit it in Zenodo for a DOI and add the DOI to [CITATION.cff](CITATION.cff) (see [MAINTAINING.md](MAINTAINING.md#releases)).

## Research pipeline

`pipeline/` is a Nextflow (DSL2) workflow that reads `data/umdb.sqlite`, never `data/` or `docs/`, so it depends only on the derived database and its rule versions:

```
data/umdb.sqlite ──SNAPSHOT──▶ samples.tsv.gz, runs.tsv.gz, snapshot.json (DB SHA-256, rule versions)
data/reference/geonames ─┐
World Bank WDI (API) ────┼─COVARIATES─▶ country/city covariates, city air routes, covariates_manifest.json
OpenFlights (optional) ──┘
snapshot ──SELECT_COHORT──▶ cohort_runs.tsv ──(Track B: SRA → fastp → seqtk → sylph/MetaPhlAn, HUMAnN, KMA, geNomad)──▶ matrices
snapshot + covariates (+ matrices) ──HYPOTHESIS × registry──▶ hypotheses/<ID>/summary.json ──REPORT──▶ report.md
```

`pipeline/assets/hypotheses.csv` lists every hypothesis with its script and required inputs; the pipeline runs those whose inputs exist and the report lists the rest. Each script shares one command-line interface (`bin/umdb_common.py`) and writes `summary.json` with a status, its result and caveats. The test profile runs everything on a synthetic fixture (`pipeline/tests/data/`) whose planted effects each test should recover. Details: [pipeline/README.md](pipeline/README.md).

## Not yet built

- **Discovery provenance:** which query profile found each experiment, and when (a `discovery_hits` table).
- **Location review queue:** a curated override file for ambiguous and conflicting locations, applied at build time.
- **Release history table** inside the database.
