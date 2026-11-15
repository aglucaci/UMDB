# UMDB Data Dictionary

Field-level reference for the files UMDB publishes and keeps. For how the annotations are derived, see [CURATION.md](CURATION.md); for how records are selected, see [INCLUSION_CRITERIA.md](INCLUSION_CRITERIA.md).

**Unit of record:** one row per sequencing **run** (SRR/ERR/DRR). One BioSample can have several runs, and one BioProject many BioSamples. Count distinct BioSamples, not runs, when comparing environments or places, or you will pseudo-replicate.

**Empty strings mean unknown.** Missing values are `""`, not `null`.

**Contents:** [What UMDB pulls from NCBI](#what-umdb-pulls-from-ncbi) · [SQLite database and releases](#sqlite-database-and-releases) · [Published files](#published-files-docs) · [Run record](#run-record) · [Working files](#working-files-data) · [Dates](#dates-which-one-to-use)

---

## What UMDB pulls from NCBI

All metadata come from NCBI E-utilities. No sequence reads are downloaded; the explorer's download scripts fetch reads separately, on demand.

### Per-run retrieval

| Step | E-utilities call | Granularity | What is kept | Where it lands |
| --- | --- | --- | --- | --- |
| 1. Discovery | `esearch` (`db=sra`) | One request per query profile per page (`crawl`), or per day paging through every hit in 10,000-ID pages (`backfill-year`, `daily`) | Matching SRA experiment UIDs | Seen list `data/seen_sra_uids.txt` |
| 2. Experiment summary | `esummary` (`db=sra`) | Batches of 200 UIDs | Experiment title, study name, SRX accession (from `ExpXml`) | `title` on each run record |
| 3. Run table | `efetch` (`db=sra`, `rettype=runinfo`) | One request per experiment; one row per run | All **47 RunInfo columns**, verbatim | `runinfo_row` |
| 4. BioSample | `efetch` (`db=biosample`) | One request per **new** BioSample; cached (failures too). `--refresh` re-fetches each once | Every submitted attribute, plus title and organism | `data/cache/biosample.json`; the location is parsed into `geo` |
| 5. BioProject | `esearch` accession → UID, then `esummary` (`db=bioproject`) | Once per **new** BioProject; cached. `--refresh` re-fetches each once | Title, description, organism, data type, dates, center, plus every raw esummary field | `data/cache/bioproject.json` (+ `bioproject_uid.json`); each run keeps a short `bioproject` summary |

Steps 4 and 5 run only with `--fetch-biosample` / `--fetch-bioproject`. All `umdb` workflows and examples pass both.

Requests are sent one at a time with up to 6 retries and exponential backoff (`ncbi.http_get`). There is no explicit rate limiter. Set `NCBI_API_KEY` so NCBI allows 10 requests per second instead of 3.

UMDB then derives and adds `geo`, `assay` and `eligibility` (at export), `ncbi` links, `debug` (exact request URLs, RunInfo column names), `provenance` (harvest time) and, when reviewed, `ai_curation`.

### Volume (current catalog)

| Item | Amount |
| --- | --- |
| Runs | 59,921 unique; about 4 KB of JSON each as harvested before 2026-11, about **2.6 KB** as slim records |
| RunInfo columns per run | 47 |
| BioSamples cached | 54,318. Attributes per sample: median 5, maximum 908; **1,947 distinct attribute names** across submitters (hence the field normalization in the geo and eligibility rules) |
| BioProjects cached | 1,609 |
| `data/srr_catalog_*.jsonl` | 230 MB |
| `data/cache/*.json` | 39 MB |
| `docs/db/` run parts | 481 MB as published in April 2026, which included duplicates; about 350 MB when rebuilt |
| `docs/db/biosamples.json`, `bioprojects.json` | 38 MB, 1.5 MB |

A deep harvest makes roughly one run-table request per experiment plus one request per new BioSample. Expect hundreds of thousands of requests per backfilled year (see [MAINTAINING.md](MAINTAINING.md#backfill-years-deepest-search)).

### Formats

| Location | Format | Layout |
| --- | --- | --- |
| `data/srr_catalog_<YEAR>.jsonl` | JSON Lines, UTF-8, one run record per line | Split into `_partNNN.jsonl` files under 50 MB. `<YEAR>` is the harvest year (`crawl`, `daily`) or the backfilled year. After a `--refresh` a run can appear more than once until `umdb compact`; exports always use the newest (latest `provenance.ingested_utc`) |
| `data/cache/*.json` | One JSON object keyed by accession | BioSample, BioProject, BioProject UID and AI caches |
| `data/seen_*.txt` | Plain text, one ID per line | De-duplication state |
| `docs/db/srr_records_partNNN.json` | JSON array of run records | Parts under 50 MB, listed in `srr_records_manifest.json` |
| `docs/db/summary.json` | JSON object | Precomputed counts for the website |
| `docs/db/biosamples.json`, `bioprojects.json` | JSON objects keyed by accession | Copies of the caches |

NCBI values are stored as NCBI returns them: RunInfo numbers are strings, dates keep NCBI's formats, and BioSample attribute names keep the submitter's spelling (`geo_loc_name`, `geographic location`, …). Normalized values exist only in the derived blocks (`geo`, `assay`, `eligibility`).

### Slim run records

Text that is identical for every run of a study is stored once, not per run:

- The BioProject **description and raw esummary fields** live only in `data/cache/bioproject.json` and `docs/db/bioprojects.json`. Each run keeps a short `bioproject` block (accession, UID, title, center, data type, submission date) for display.
- The 47 RunInfo **column names** are no longer repeated in `debug`.
- Eligibility evidence from BioProject text records the field and matched terms, not the text itself.

New harvests write slim records; `umdb export` slims what it publishes, and `umdb compact` slims `data/`. On the current catalog the run files drop from 230 MB to 152 MB, and the published run parts from 481 MB (with the double-counted rows) to 243 MB in 5 files. Look up a run's full project text by `bioproject.accession` in `bioprojects.json`.

---

## SQLite database and releases

`data/umdb.sqlite` (built by `umdb db build`, git-ignored) and the Parquet/TSV releases (`umdb release`) hold the same information in normalized tables: `projects`, `samples`, `experiments`, `runs`, `project_samples`, `ai_annotations`, plus the views `umdb_samples` and `umdb_runs`. Column names follow the record fields below (for example `samples.urban_status` = `eligibility.status`, `samples.city` = `geo.city`, `runs.release_date` = `runinfo_row.ReleaseDate`). Unknown values are NULL. Every release's `schema.json` lists each file's columns and types. Table-by-table description, indexes and example queries: [ARCHITECTURE.md](ARCHITECTURE.md#sqlite-schema-version-100).

---

## Published files (`docs/`)

| File | Contents |
| --- | --- |
| `docs/db/srr_records_manifest.json` | Index of the current release's part files |
| `docs/db/srr_records_partNNN.json` | JSON arrays of run records (schema below), each under 50 MB |
| `docs/db/srr_index.json` | Compact copy of the manifest for the website |
| `docs/db/summary.json` | Precomputed counts used by the home and analytics pages |
| `docs/db/bioprojects.json` | BioProject metadata keyed by accession |
| `docs/db/biosamples.json` | BioSample attributes keyed by accession |
| `docs/db/ai_curation.json` | AI review results keyed by run accession (when present) |
| `docs/latest_srr.json` | Runs added by the most recent harvest |
| `docs/debug/latest_report.json` | Counters, queries and query-profile version from the most recent run |

### `srr_records_manifest.json`

| Field | Type | Description |
| --- | --- | --- |
| `generated_utc` | ISO 8601 | When the export ran |
| `total_records` | int | Runs across all parts |
| `parts[].path` | string | Repository-relative path of a part file |
| `parts[].records` | int | Runs in that part |
| `catalog_file_years` | int[] | Years in the `data/srr_catalog_<YEAR>.jsonl` filenames, i.e. **harvest** years. Not run release years. Called `years` before 2026-11 |

### `summary.json`

| Field | Description |
| --- | --- |
| `totalRuns`, `totalBioSamples`, `totalProjects` | Distinct counts |
| `totalCountries`, `totalCities` | Distinct non-empty labels (text labels, not validated places) |
| `geoResolvedRuns` | Runs with both a country and a city label |
| `downloadableRuns` | Runs with an SRA `download_path` |
| `years` | `[{name, count}]` by **run release year** (`ReleaseDate`, falling back to `LoadDate`) |
| `assays`, `countries`, `cities`, `centers` | `[{name, count}]` tallies of runs |
| `eligibility` | `[{name, runs, biosamples}]` per eligibility tier (five tiers) |
| `largestProject`, `topProjects` | Per-BioProject rollups (run, BioSample, country, city and assay counts) |

Country, city and assay values in the summary use the effective annotation (see [precedence](CURATION.md#annotation-layers-and-precedence)).

---

## Run record

### Top level

| Field | Type | Description |
| --- | --- | --- |
| `srr` | string | Run accession (SRR, ERR or DRR) |
| `sra_uid` | string | NCBI SRA internal UID of the experiment the run belongs to |
| `title` | string | SRA experiment title, from esummary `ExpXml`. Empty for runs harvested before 2026-11 |
| `runinfo_row` | object | SRA RunInfo row, verbatim (see below) |
| `geo` | object | Derived geographic context (see below) |
| `assay` | object | Derived assay class (see below) |
| `bioproject` | object | BioProject metadata (see below) |
| `ncbi` | object | Links: `sra_uid_url`, `srr_url`, `bioproject_url` |
| `debug` | object | `runinfo_url`, `biosample_url`: the exact requests used (older records may also list `runinfo_columns`) |
| `provenance` | object | `ingested_utc` (harvest time), `source` (`ncbi_eutils`) |
| `eligibility` | object | Rule-based urban eligibility tier and evidence, assigned at export (see below) |
| `ai_curation` | object | Present only if the run was AI-reviewed; merged at export (see below) |

### `runinfo_row` (selected fields)

Copied unchanged from SRA RunInfo, so all values are strings. The full column list is in `debug.runinfo_columns`.

| Field | Description |
| --- | --- |
| `Run`, `Experiment`, `Sample`, `SRAStudy` | SRR / SRX / SRS / SRP accessions |
| `BioProject`, `BioSample` | PRJNA… / SAMN… accessions |
| `ReleaseDate`, `LoadDate` | When the run became public / was loaded. **Not** collection date |
| `LibraryStrategy`, `LibrarySource`, `LibrarySelection`, `LibraryLayout` | Submitter-declared library fields (INSDC vocabulary); primary input to the assay class |
| `Platform`, `Model` | Sequencer |
| `spots`, `bases`, `avgLength`, `size_MB` | Run size |
| `ScientificName`, `TaxID` | Organism or metagenome label (e.g. `wastewater metagenome`) |
| `CenterName` | Submitting center. Says nothing about where the sample was collected |
| `download_path` | Direct SRA download URL |

### `geo`

Derived from the BioSample `geo_loc_name` attribute (INSDC format `Country: free text`). Text-parsed only: **not validated against a gazetteer.**

| Field | Description |
| --- | --- |
| `raw` | The original location string, verbatim |
| `raw_field` | Which BioSample attribute `raw` came from (e.g. `geo_loc_name`) |
| `country` | Normalized country name (e.g. `USA` → `United States`) |
| `country_source` | `biosample`, `text_fallback` (inferred from title or sample name), or empty |
| `region` | First admin-level token: state, province, county, prefecture |
| `city` | A locality token. Never a state, province, county or placeholder. May be a campus, park or neighbourhood |
| `city_status` | `parsed` (one locality token), `ambiguous` (several; the first was kept), `region_only`, `placeholder` (e.g. `multiple cities`), `missing` |
| `locality_tokens` | Every locality-like token found, in order |
| `parse_flags` | Notes on the parse, e.g. `no_colon_delimiter`, `continent_token`, `city_same_name_as_state` |
| `resolution` | Currently always `text_parsed_unvalidated` |
| `lat`, `lon` | Decimal degrees from `lat_lon`, when present and in range. Hemisphere letters are converted to signs |
| `collection_date` | BioSample `collection_date`, verbatim (formats vary) |
| `biosample_accession` | BioSample the values came from |

`raw_field` through `collection_date` were added in 2026-11. Releases exported earlier have only `country`, `region`, `city`, `lat`, `lon`, `raw` and `biosample_accession`, with `city` taken positionally (so it can hold a state).

### `assay`

| Field | Description |
| --- | --- |
| `assay_class` | `WGS`, `Metatranscriptomics`, `RNA-seq`, `Viral RNA`, `16S`, `18S`, `ITS`, `Amplicon`, or `Unknown` (definitions in [CURATION.md](CURATION.md#assay-classes)) |
| `assay_tags` | Extra descriptors, e.g. `shotgun`, `amplicon`, `viral`, `targeted-capture` |
| `confidence` | `high` (from structured library fields), `medium` (from free text), `low` |
| `rationale` | The rule inputs that fired, e.g. `["source=METATRANSCRIPTOMIC"]` |

### `bioproject`

On each run record (short form):

| Field | Description |
| --- | --- |
| `accession`, `uid` | PRJNA… accession and NCBI UID |
| `title` | Project title |
| `center_name`, `data_type` | Registering center, data type |
| `submission_date` | Registration date (not a collection date) |
| `error` | Set when lookup failed (`invalid_accession`, `uid_not_found`) |

The full record in `bioprojects.json` / `data/cache/bioproject.json`, keyed by accession, adds `description` (abstract), `organism`, `last_update`, `ncbi.bioproject_url` and `esummary_items` (raw esummary fields, verbatim). Records harvested before 2026-11 may still carry these on the run until the next `umdb compact`.

### `eligibility`

Assigned per run by [`eligibility.py`](scripts/umdb_harvester/eligibility.py) at export. Rules: [INCLUSION_CRITERIA.md § 3](INCLUSION_CRITERIA.md#3-the-eligibility-screen). Records exported before 2026-11 have no `eligibility`; the explorer treats them as `unscreened`.

| Field | Description |
| --- | --- |
| `status` | `confirmed_urban`, `probable_urban`, `uncertain`, `non_urban` or `non_environmental` |
| `reasons` | The decision rule that fired, e.g. `named urban infrastructure at sample level` |
| `environmental_sample` | `true` when there is environmental-sample evidence and the run is not non-environmental |
| `urban_context` | Up to three of `transit`, `built_environment`, `municipal_wastewater`, `air_dust`, `soil_greenspace`, `water_drainage`, `surfaces_infrastructure`, strongest first. Empty unless confirmed or probable |
| `location_resolution` | `coordinate`, `city`, `region`, `country` or `unknown` |
| `location_validation` | `plausible`, `ambiguous`, `unverified` or `missing` (no gazetteer check yet, so never `verified`) |
| `evidence[]` | Each item: `source` (`SRA`, `BioSample`, `BioProject`), `field`, `value` (first 200 characters; omitted for BioProject text, which is in `bioprojects.json`), `signal`, and the matched `terms` |
| `evidence[].signal` | `environmental`, `non_environmental`, `laboratory`, `urban_explicit`, `urban_infrastructure`, `urban_location`, `non_urban_site`, `project_urban`, `project_non_urban` |
| `decision_method` | `rules` |
| `rules_version` | Version of the rule set |

### `ai_curation`

Stored in `data/cache/ai_curation.json`, keyed by run, and attached at export. Model output, not expert review. See [CURATION.md](CURATION.md#ai-assisted-review).

| Field | Description |
| --- | --- |
| `reviewed` | `false` when the call failed (then `error` is set) |
| `model`, `reviewed_utc` | Model id and time |
| `source_signature` | The exact context (serialized JSON) the model saw; a review re-runs when this changes |
| `metadata_sufficiency` | `sufficient`, `partial`, `insufficient`; `metadata_sufficient` is its boolean form |
| `urban_origin` | `urban`, `non_urban`, `uncertain` |
| `confidence` | `low`, `medium`, `high`: the model's self-report, **not** a calibrated probability |
| `environment_type` | Free-text environment label |
| `original_country/city/assay_class` | Rule-derived values the model was shown |
| `final_country/city/assay_class` | Values after review (equal to the originals when unchanged) |
| `change_country/city/assay_class`, `ai_fixed` | Whether the model changed each value / any value |
| `reasoning_summary`, `evidence` | Model's justification (up to 8 short strings) |

---

## Working files (`data/`)

| File | Format | Description |
| --- | --- | --- |
| `srr_catalog_<YEAR>.jsonl` (+ `_partNNN`) | JSON Lines, one run record per line | Canonical catalog. `<YEAR>` is the harvest year |
| `seen_sra_uids.txt`, `seen_srr_runs.txt` | One ID per line | IDs already ingested; used to skip duplicates |
| `cache/biosample.json` | `{accession: {attributes, title, organism, efetch_url}}` | BioSample cache |
| `cache/bioproject.json`, `cache/bioproject_uid.json` | JSON | BioProject cache, and accession → UID map |
| `cache/ai_curation.json` | `{srr: ai_curation}` | AI review cache; created by the first AI curation run |

## Dates: which one to use

| Question | Field |
| --- | --- |
| When was the sample collected? | `geo.collection_date` (or the BioSample `collection_date` attribute) |
| When did the data become public? | `runinfo_row.ReleaseDate` |
| When was the project registered? | `bioproject.submission_date` |
| When did UMDB harvest it? | `provenance.ingested_utc` |

These are different dates and are often years apart. `summary.json` year charts use `ReleaseDate`.
