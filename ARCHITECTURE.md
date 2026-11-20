# UMDB data architecture

How UMDB stores, queries and releases its data, and why. UMDB is the harvesting and release repository; analyses run in [STRIGA](https://github.com/aglucaci/STRIGA) and the website is [UMA](https://github.com/aglucaci/UMA). Both read only what UMDB hands them: STRIGA a catalog (`data/` or a release), UMA a release. Field definitions are in [DATA_DICTIONARY.md](DATA_DICTIONARY.md). Day-to-day operation is in [MAINTAINING.md](MAINTAINING.md).

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
        │                                    │
        │                                    │  nextflow run aglucaci/STRIGA --catalog data
        │                                    ▼
        │                              STRIGA results/ (+ World Bank, OpenFlights, … covariates)
        │                                    │  striga_manifest.json: STRIGA version, database SHA-256
        │  umdb release --analysis results/  │
        ▼◄───────────────────────────────────┘
dist/releases/<version>/  +  umdb-<version>.tar.gz     ← release contract umdb-release/1
  Parquet + TSV + SQLite + collections + GeoNames reference + STRIGA results + manifest + checksums
  → GitHub Releases (tag v<version>) / Zenodo (DOI)
        │  uma fetch / uma build (UMA repository)
        ▼
aglucaci.github.io/UMA                                 ← website data built from one release
```

## Which format does what

| Layer | Format | Location | Role |
| --- | --- | --- | --- |
| Source of truth | JSON Lines + JSON caches | `data/` (git-ignored), bundled in the `catalog-latest` release | Append-only ingestion log of slim run records, plus raw BioSample/BioProject responses. One 18x-compressed bundle with checksums; pull and push from any checkout or workflow |
| Query database | SQLite (`umdb.sqlite`) | `data/` (git-ignored) | Normalized, indexed, full-text searchable, with gazetteer-validated locations. Rebuilt deterministically from the catalog and the GeoNames extract, with the current geography, assay and eligibility rules |
| Gazetteer | GeoNames text/zip files | `data/reference/geonames/` (git-ignored), bundled with the catalog | ISO countries, admin1 regions, populated places (≥ 500 people). Downloaded once (`umdb geo fetch`); checksums and download date recorded in the database `meta`; shipped in every release (`reference/geonames/`) |
| Releases | Parquet (Zstandard), gzipped TSV, compressed SQLite, JSON | `dist/releases/<version>/` (not in git; GitHub Releases, Zenodo) | Compact, typed, versioned snapshots with checksums, for DuckDB, R, Python or spreadsheets, plus the STRIGA results for that catalog. What STRIGA and UMA read |
| Website | Compact JSON | the UMA repository (`docs/db/`, GitHub Pages) | Built by UMA from one release; see UMA's ARCHITECTURE.md |

**Why SQLite is derived, not stored.** A binary file that changes on every harvest makes a poor git object: each change adds the whole file to history, and git cannot diff or merge it. Keeping the slim JSONL as the record means the database can always be rebuilt identically.

**Why the catalog is in Release assets, not git.** After the 2026 deep backfill the JSONL catalog passed 1 GB, and every harvest rewrites large parts of it, so each commit added hundreds of MB to history. As one gzipped bundle it is about 18 times smaller, and replacing a release asset adds nothing to the repository. `umdb catalog push` refuses to overwrite a bundle uploaded by someone else since your last pull; `pull --merge` combines them (append-only log, newest record per run wins).

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
pip install -r requirements-release.txt                  # pyarrow (+ duckdb for analysis); once
umdb release --version 1.0.0 --analysis ../STRIGA/results  # --analysis is optional
```

This writes `dist/releases/1.0.0/` and the single-file bundle `dist/releases/umdb-1.0.0.tar.gz`:

| Path | Contents |
| --- | --- |
| `manifest.json` | `contract` (`umdb-release/1`), version, DOI, build time, schema, query-profile and rules versions, licences, row counts, `analysis` (STRIGA version, commit, hypothesis statuses), and size and SHA-256 of every file |
| `parquet/` | Normalized tables (`projects`, `samples`, `experiments`, `runs`, `project_samples`, `ai_annotations`, `countries`, `admin1`, `places`, `sample_locations`; verbatim RunInfo and raw esummary JSON omitted), the convenience tables `umdb_samples` and `umdb_runs`, and the curated layers `studies`, `locations`, `curation_evidence`, `environmental_metadata` |
| `tsv/` | `umdb_samples.tsv.gz`, `umdb_runs.tsv.gz` |
| `umdb.sqlite.zst` (or `.gz`) | The query database itself |
| `collections/` | `collections.json` (definitions and statistics) and `<slug>.tsv.gz` run lists |
| `indexes/coverage.json` | Urban BioSamples, BioProjects and places per country, with population |
| `reference/geonames/` | `countryInfo.txt`, `cities500.zip`: the gazetteer the locations were validated against |
| `documentation/` | Data dictionary, architecture, inclusion criteria, curation notes, licence, `cohort-templates.json` |
| `analysis/` | STRIGA results for this exact database (`striga_manifest.json`, `report.md`, `report.json`, `hypotheses_status.tsv`, `hypotheses/<ID>/`, `cohort/`, `covariates/` with the outside sources and their checksums, `profiles/`) |
| `schema.json`, `checksums.sha256`, `.zenodo.json` | Columns and types; `shasum -a 256 -c checksums.sha256`; Zenodo deposit metadata |

### The release contract

`umdb-release/1` is what the other two repositories rely on; changing it means a new contract version and a matching STRIGA or UMA update.

* **STRIGA** reads `umdb.sqlite[.zst|.gz]` and `reference/geonames/` with `--catalog <folder>`. A UMDB checkout's `data/` folder has the same layout, which is how a release's analyses are run before the release exists.
* **STRIGA → UMDB.** STRIGA writes `striga_manifest.json` (contract `striga-results/1`) with the SHA-256 of the uncompressed database it analysed. `umdb release --analysis` copies the results into `analysis/` only if that hash equals `data/umdb.sqlite`'s, so results and catalog always match. Results from a run without hashing need `--allow-unverified-analysis`, and the manifest records `database_verified: false`.
* **UMA** reads `manifest.json`, the database, `collections/`, `documentation/cohort-templates.json`, `reference/geonames/countryInfo.txt` and `analysis/`, and refuses a release whose contract it does not know.

Parquet files use Zstandard compression, typed integers, floats and dates, and NULL for unknown values. Analyse them directly with DuckDB:

```sql
SELECT urban_context, count(*) AS samples
FROM read_parquet('dist/releases/1.0.0/parquet/umdb_samples.parquet')
WHERE urban_status IN ('confirmed_urban', 'probable_urban')
GROUP BY 1 ORDER BY samples DESC;
```

Publishing (GitHub Release, Zenodo DOI, UMA rebuild): [MAINTAINING.md](MAINTAINING.md#releases).

## Not yet built

- **Discovery provenance:** which query profile found each experiment, and when (a `discovery_hits` table).
- **Location review queue:** a curated override file for ambiguous and conflicting locations, applied at build time.
- **Release history table** inside the database.
