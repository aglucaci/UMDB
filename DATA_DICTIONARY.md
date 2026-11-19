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
| `docs/db/` | Compact index about 2 MB (0.4 MB compressed) plus per-project files; replaced run parts that reached over 1 GB |

A deep harvest makes roughly one run-table request per experiment plus one request per new BioSample. Expect hundreds of thousands of requests per backfilled year (see [MAINTAINING.md](MAINTAINING.md#backfill-years-deepest-search)).

### Formats

| Location | Format | Layout |
| --- | --- | --- |
| `data/srr_catalog_<YEAR>.jsonl` | JSON Lines, UTF-8, one run record per line | Split into `_partNNN.jsonl` files under 50 MB. `<YEAR>` is the harvest year (`crawl`, `daily`) or the backfilled year. After a `--refresh` a run can appear more than once until `umdb compact`; exports always use the newest (latest `provenance.ingested_utc`) |
| `data/cache/*.json` | One JSON object keyed by accession | BioSample, BioProject, BioProject UID and AI caches |
| `data/seen_*.txt` | Plain text, one ID per line | De-duplication state |
| `docs/db/cube.json`, `projects.json`, `p/<ACC>.json` | Compact JSON, dictionary-encoded columns | Website data built from `data/umdb.sqlite` (see [Published files](#published-files-docs)) |
| `docs/db/summary.json` | JSON object | Precomputed counts for the website |

`data/` catalog files are **not in git**: they live in GitHub Release assets (`catalog-latest`) and are restored with `umdb catalog pull` (see [MAINTAINING.md](MAINTAINING.md#the-catalog-lives-in-release-assets)).

NCBI values are stored as NCBI returns them: RunInfo numbers are strings, dates keep NCBI's formats, and BioSample attribute names keep the submitter's spelling (`geo_loc_name`, `geographic location`, …). Normalized values exist only in the derived blocks (`geo`, `assay`, `eligibility`).

### Slim run records

Text that is identical for every run of a study is stored once, not per run:

- The BioProject **description and raw esummary fields** live only in `data/cache/bioproject.json` (and in the SQLite `projects` table and each project's `docs/db/p/<ACC>.json`). Each run keeps a short `bioproject` block (accession, UID, title, center, data type, submission date) for display.
- The 47 RunInfo **column names** are no longer repeated in `debug`.
- Eligibility evidence from BioProject text records the field and matched terms, not the text itself.

New harvests write slim records; `umdb export` slims what it publishes, and `umdb compact` slims `data/`. On the current catalog the run files drop from 230 MB to 152 MB, and the published run parts from 481 MB (with the double-counted rows) to 243 MB in 5 files. Look up a run's full project text by `bioproject.accession` in `data/cache/bioproject.json` or the `projects` table.

---

## SQLite database and releases

`data/umdb.sqlite` (built by `umdb db build`, git-ignored) and the Parquet/TSV releases (`umdb release`) hold the same information in normalized tables: `projects`, `samples`, `experiments`, `runs`, `project_samples`, `ai_annotations`, the location tables `countries`, `admin1`, `places` and `sample_locations`, plus the views `umdb_samples` and `umdb_runs`. Column names follow the record fields below (for example `samples.urban_status` = `eligibility.status`, `samples.city` = `geo.city`, `runs.release_date` = `runinfo_row.ReleaseDate`). Unknown values are NULL. Every release's `schema.json` lists each file's columns and types. Table-by-table description, indexes and example queries: [ARCHITECTURE.md](ARCHITECTURE.md#sqlite-schema-version-110).

### Validated locations

`samples.country`, `samples.region` and `samples.city` are the parser's text labels, kept for provenance. **Use `sample_locations` for anything you count, filter or map.** It holds one accepted, gazetteer-validated location per BioSample (rules in `scripts/umdb_harvester/geo.py`, version in `meta.geo_rules_version`).

| Table / column | Description |
| --- | --- |
| `countries` | ISO 3166-1: `iso2` (key), `iso3`, `iso_numeric`, `name`, `continent` (GeoNames countryInfo) |
| `admin1` | First-order regions: `code` (GeoNames admin1 code such as `US.NY`), `country_iso2`, `name`, `geonames_id` |
| `places` | GeoNames populated places used by at least one sample: `geonames_id` (key), `name`, `country_iso2`, `admin1_code`, `lat`, `lon`, `population`, `feature_code` |
| `sample_locations.country_iso2` | Accepted country, or NULL |
| `sample_locations.country_status` | `verified` (from the BioSample location field), `probable` (loose alias, text fallback, or coordinates only), `conflicting` (precise coordinates lie in another country), `unresolved` |
| `sample_locations.admin1_code` | Region, from the text or from the accepted place |
| `sample_locations.place_id` | Accepted GeoNames place; set only when `status` is `verified` or `probable` |
| `sample_locations.status` | `verified`: the text names exactly one place in the country (after any region constraint), and precise coordinates, if reported, lie within 30 km (more for large cities). `probable`: chosen among same-named places by population, the region's main city, inferred from precise coordinates alone (the most populous settlement whose population-scaled extent, about 2.5 km for a village up to 25 km for a large city, contains the point; never an administrative unit such as a county), or a city-state. `ambiguous`: several places match. `conflicting`: precise coordinates are far from the only place named. `unresolved`: no place identified |
| `sample_locations.method` | `text`, `text+coordinates`, `text-population`, `coordinates` or `city-state` |
| `distance_km`, `coord_decimals`, `matched_text`, `note` | Evidence: reported coordinates to the place, their precision (whole degrees never verify or contradict), the location token that named the place, and a human-readable reason |

The database enforces the rules with CHECK constraints and a trigger: a place requires an accepted status and an accepted country, every verified or probable record has a place, and a place or region must lie in the record's country. GeoNames data are CC BY 4.0; cite GeoNames when publishing place-level results.

---

## Published files (`docs/`)

The website loads a compact index built from `data/umdb.sqlite` (`umdb export`, and after every harvest), not every run record. Opening a BioProject fetches that project's file. Full tables are in the Parquet releases. All geography in these files comes from `sample_locations`: a country is published when `country_status` is verified or probable, a place when `status` is verified or probable. AI proposals never change counts or filters.

| File | Contents |
| --- | --- |
| `docs/db/summary.json` | Exact counts from SQLite, each labelled with its unit |
| `docs/db/geo.json` | Validated location hierarchy: countries, regions and places used by samples |
| `docs/db/samples.json` | BioSamples aggregated so that each sample is counted in exactly one row |
| `docs/db/cube.json` | Runs aggregated by the same dimensions |
| `docs/db/projects.json` | One row per BioProject |
| `docs/db/p/<ACC>.json` | One BioProject's samples and runs (`_none.json` for runs without a BioProject) |
| `docs/db/ix/<NN>.json` | Accession lookup: every BioSample and run accession → its BioProject (index into `projects.json` rows), split into 100 files by the accession's last two digits (`xx` if it has none). Lets `sample.html` and `run.html` find the right project file |
| `docs/db/collections.json` | Curated collections: definition (Explorer filters), inclusion criteria, suggested use and workflow, and summary statistics |
| `docs/db/collections/<slug>.tsv.gz` | One collection's runs: run, biosample, project, assay, layout, platform, bases, country, GeoNames id, city, habitat, collection date |
| `docs/db/population.json` | Every country's population (GeoNames), for per-capita map layers, including countries with no samples |
| `docs/db/analytics.json` | Precomputed catalog structure for the Analytics page: study concentration (Lorenz curve, Gini), collection-to-release delay, library sizes by assay, metadata completeness by assay |
| `docs/assets/cohort-templates.json` | README, download script and starter analyses of a research package; written by `cohorts.py` so the browser and the CLI export the same files |
| `docs/db/manifest.json` | Build time, schema, rule, query and gazetteer versions, counts, file sizes |
| `docs/db/srr_records_manifest.json` | Kept for older loaders: `generated_utc`, `total_records`, empty `parts` |
| `docs/latest_srr.json` | Runs added by the most recent harvest |
| `docs/debug/latest_report.json` | Counters, queries and query-profile version from the most recent run |

Unchanged files are not rewritten, so a harvest changes only the projects it touched. Download URLs and verbatim NCBI records are left out (they are in the database and releases).

### `geo.json`

`countries` rows (`country_cols`: iso2, numeric, name, continent, population), `admin1` rows (code, index into `countries`, name) and `places` rows (geonames_id, name, index into `countries`, index into `admin1` or null, lat, lon, population). `statuses` lists the location statuses that `loc` columns index into. `attribution` carries the GeoNames credit.

### `samples.json` (unit: BioSamples)

`cols` names the columns of each row in `rows`; `dims` holds the value lists that dictionary-encoded columns index into.

| Column | Type | Description |
| --- | --- | --- |
| `projects` | int → `dims.projsets` | The set of BioProjects the sample belongs to (each a list of indexes into `dims.project`) |
| `year` | int or null | SRA release year of the sample's earliest run |
| `cyear` | int or null | Collection year (`collection_date`) |
| `country`, `admin1`, `place` | int → `geo.json` lists, or null | Accepted location |
| `assays` | int → `dims.assaysets` | The set of assay classes among the sample's runs |
| `tier` | int → `dims.tier` | Sample-level eligibility tier |
| `context` | int → `dims.context` | Primary urban context (`""` = none) |
| `loc` | int → `dims.loc` | Place status (verified, probable, ambiguous, conflicting, unresolved) |
| `lat`, `lon` | float or null | Reported coordinates rounded to 0.01°, about 1 km (not inferred from place names) |
| `samples` | int | BioSamples in the row |

Because project and assay are sets, a sample in two projects or with runs of two assays is still one row: `sum(samples)` over any filter is exact.

### `cube.json` (unit: runs)

Columns `project`, `year` (run release year), `cyear`, `country`, `admin1`, `place`, `assay`, `tier` (run-level), `context`, `loc`, `ai` (index into `dims.ai`, or null), `runs`, `gbases`. It no longer has a `samples` column: count BioSamples with `samples.json`.

### `projects.json` and `p/<ACC>.json`

`projects.json`: `cols` = accession, title, center_name, data_type, submission_date, runs, samples, first_year, last_year.

`p/<ACC>.json`: `project` (accession, title, description, organism, data type, centre, dates); `samples` rows with `sample_cols` (accession, title, collection date, accepted country name, region name and place name, lat, lon, urban_status, urban_context, eligibility_reason, isolation_source, env_broad_scale, env_local_scale, env_medium, host, location_resolution, country_iso2, place_id, location_status, location_method, location_note, the submitted `geo_loc_raw`, collection_year); `runs` rows with `run_cols` (accession, index into `samples`, experiment, assay and library fields, platform, instrument, release date, spots, bases, average length, size in MB, organism label, run-level tier, experiment title); `ai` keyed by run accession (proposals, `status: "proposed"`).

### `summary.json`

| Field | Description |
| --- | --- |
| `units` | What each count means |
| `totalRuns`, `totalBioSamples`, `totalProjects` | Distinct counts (`runsWithoutProject` separately) |
| `totalCountries`, `totalCities` | Distinct accepted countries and GeoNames places |
| `geoResolvedRuns`, `geoResolvedBioSamples` | Runs and BioSamples with an accepted place |
| `location` | BioSamples per place status and per country status |
| `eligibility` | `[{name, runs, biosamples}]` per tier |
| `years` | `[{name, count}]` runs by **release year** |
| `collectionYears` | `[{name, count}]` BioSamples by **collection year** |
| `assays` | `[{name, count, biosamples}]` (`count` = runs) |
| `countries` | `[{iso2, name, biosamples, runs, count}]` (`count` = runs, for older pages) |
| `cities` | Top 200 `[{geonames_id, name, country, biosamples, runs, count}]` |
| `centers` | `[{name, count}]` runs per submitting centre |
| `largestProject`, `topProjects` | Largest BioProjects by runs |

### Research packages (cohorts)

The Workspace (`workspace.html`) and `umdb cohort export|collection` write the same package. A cohort is defined by a JSON document: either `{"filters": {…}}`, with the Explorer's filters (`tier`, `context`, `assay`, `country`, `region`, `city`, `loc`, `cmin`/`cmax` collection years, `ymin`/`ymax` release years, `project`, `q`), or a set operation `{"op": "union" | "intersection" | "difference", "a": …, "b": …}`. Optional `balance`: `max_per_cell` (per city × habitat; country when no city), `max_per_project`, `target`, `seed`. Balancing sorts samples by accession, shuffles them with a seeded mulberry32 generator and keeps samples while the caps allow, identically in Python and JavaScript.

| File | Contents |
| --- | --- |
| `cohort.json` | `name`, `release` (database build time and rule versions), `definition` (filters or set operation, balance, seed), `counts`, file list |
| `metadata.tsv` | One row per BioSample: biosample, bioproject (lowest accession if several), urban_status, habitat, country_iso2, country, region, place_id, city, lat, lon, location_status, collection_date (full ISO dates only), collection_year, collection_date_raw, assays, n_runs |
| `runs.tsv` | run, biosample, project, library_layout, bases, spots, instrument_model, assay_class, platform, library_strategy, release_date. Runs are limited to the assays the cohort filters on. Also the `--cohort` input of the UMDB-SCIENCE pipeline |
| `accessions.txt` | Run accessions |
| `download_fastq.sh` | SRA Toolkit download of every run into `fastq/` |
| `samplesheet.csv` | nf-core/taxprofiler format (`sample,run_accession,instrument_platform,fastq_1,fastq_2,fasta`) |
| `cohort_summary.json` | Counts by habitat, country, collection year, assay and layout |
| `analysis.py`, `analysis.R`, `CITATION.bib`, `README.md` | Starter analyses, citation and a description of the package |
| `metadata.parquet` | CLI only, when pyarrow is installed |

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

Git-ignored; stored as one bundle in GitHub Release assets (`umdb catalog push` / `pull`).

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
