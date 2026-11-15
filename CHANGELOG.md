# Changelog

User-visible changes to the UMDB pipeline, data schema and website. Data refreshes from scheduled crawls are not listed individually.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Tagged releases will follow semantic versioning once versioned database releases begin (see [MAINTAINING.md](MAINTAINING.md#releases)).

## [Unreleased]

### Added
- SQLite query database `data/umdb.sqlite` (schema 1.0.0), derived from the git JSONL catalog and caches: normalized `projects` / `samples` / `experiments` / `runs` / `project_samples` / `ai_annotations` tables with BioSample as the unit of observation, cohort and foreign-key indexes, an FTS5 full-text index, and `umdb_samples` / `umdb_runs` views. Built in about 30 s by `umdb db build`, and automatically after every harvest (`--no-db` skips it). Query with `umdb db info|shell|query`.
- Versioned releases: `umdb release` writes Parquet tables (Zstandard, typed columns, NULL for unknown), gzipped TSV, `schema.json`, `manifest.json` and `checksums.sha256` to `dist/releases/<version>/`. The whole catalog is about 11 MB, versus 243 MB of website JSON. Optional dependency: `requirements-release.txt` (pyarrow, duckdb).
- `ARCHITECTURE.md`: data flow, schema, example SQL and DuckDB queries, release process.
- Verbose, colour-coded output for every command: startup settings and catalog state, per-day/per-page hits, ingest progress with ETA, export steps with eligibility tiers, a closing summary with NCBI request statistics, and retry and failure warnings. `-v`/`--verbose` adds every request and cache hit; `-q`/`--quiet` shows summaries only (`UMDB_VERBOSITY`, `NO_COLOR`, `UMDB_COLOR`). Backfill logs stay plain text.
- Deepest-search mode: `umdb deepest` (= `scripts/deep_backfill.sh --deepest`) harvests 2007 to now year by year with every profile, paging through every hit per day (`--max-per-day 0`, now the default). It re-fetches records already held (`--refresh` on `crawl`, `daily` and `backfill-year`) and compacts the catalog after each year (`umdb compact`), keeping only the newest record per run. Exports publish only the newest record per run. Refresh and compaction ask before overwriting uncommitted data.
- `umdb` command (`bin/umdb`, install with `bin/umdb install`): `umdb crawl|daily|backfill-year|curate-ai` (or `umdb run …`), `umdb backfill`, `export`, `serve`, `status`, `queries`, `test`. Prints an ASCII-art banner in interactive terminals (`UMDB_NO_BANNER=1` hides it).
- Sample-level urban eligibility screen (`eligibility.py`), following a staged model: environmental eligibility, then urban-origin verification, then annotation. Every run gets `eligibility.status` (`confirmed_urban` / `probable_urban` / `uncertain` / `non_urban` / `non_environmental`), an urban context category, location resolution, and structured evidence (source, field, value, signal). Uses `building_setting` and `built_struc_set` when present. A city name alone gives at most `probable_urban`. Tier counts are in `summary.json`, and the explorer has an **Urban eligibility** filter and a per-run label. Current catalog: 3,362 confirmed, 17,209 probable, 18,232 uncertain, 2,093 non-urban and 19,025 non-environmental runs. No records are removed.
- `scripts/deep_backfill.sh`: resumable year-by-year harvest with every profile at NCBI's 10,000-per-day maximum, per-year logs and completion markers, optional per-year commits.
- `backfill-year` warns when a day/profile search hits `--max-per-day`.
- `INCLUSION_CRITERIA.md`: how records are selected as urban (keyword search only), exact queries, measured composition, filtering recipes. Linked from the README, the methodology page and the curation docs.
- `LICENSE`: CC BY-NC-SA 4.0 for code, documentation, annotations and data; declared in `CITATION.cff` and the README.
- `CONTRIBUTING.md`, `MAINTAINING.md`, `DATA_DICTIONARY.md`, `CURATION.md`, `SECURITY.md`, `CITATION.cff`, issue and pull request templates.
- Settings file support: the harvester loads a git-ignored `.env` from the repository root; template in `.env.example`. Shell variables and CI secrets take precedence.
- Offline unit tests (`tests/`), run with `python3 -m unittest discover tests`.
- `QUERY_PROFILES_VERSION`, recorded in `docs/debug/latest_report.json` with each run.
- New `geo` fields: `raw_field`, `country_source`, `city_status`, `locality_tokens`, `parse_flags`, `resolution`, `collection_date`.
- New assay classes `Metatranscriptomics` and `Viral RNA`, plus `18S`.

### Changed
- Slim run records: the BioProject description and raw esummary fields are stored once per project (`bioprojects.json`), not on every run, and the 47 RunInfo column names are no longer repeated. BioProject evidence in `eligibility` keeps terms, not text. New harvests write slim records, exports publish them, and `umdb compact` slims `data/`. Current catalog: run files 230 → 152 MB; published run parts 481 → 243 MB (5 files instead of 10), including the double-count fix. The eligibility screen still reads full project text from the cache.
- Discovery queries rewritten (`QUERY_PROFILES_VERSION` 2026.11.2): seven urban-environment profiles (`municipal_wastewater`, `air_dust`, `built_environment`, `urban_metagenome`, `transit`, `urban_soil_greenspace`, `urban_water_drainage`) built on NCBI environment organism labels and urban phrases, with no catch-all terms and no global NOT block. SARS-CoV-2 targeted amplicon surveillance is the opt-in `wastewater_sars_cov_2` profile. 2025 matches drop from roughly 600,000+ to 64,173 experiments, while 98% of 2024 confirmed-urban and 95% of probable-urban catalog runs are still found, and 99.5% of non-environmental runs no longer match. The old profiles remain as `LEGACY_QUERY_PROFILES`.
- Eligibility screen: "X City" place names no longer count as explicit urban wording (rules 2026.11.6).
- Renamed the harvester package `scripts/urbanscope_harvester` → `scripts/umdb_harvester`, the workflows to `umdb_*.yml`, and the NCBI tool name to `umdb-srr-harvester`. Older harvested records still contain `tool=urbanscope-srr-harvester` in their stored request URLs.
- Exports now re-derive `geo` and `assay` for every record from cached metadata, so rule fixes apply to the whole catalog.
- Assay classification trusts `LibraryStrategy` and `LibrarySource` before free text.
- Geography parsing classifies tokens instead of taking the last comma-separated token as the city. Effect on the catalog's 59,921 unique runs: runs with a country and city 26,209 → 20,128; distinct city labels 1,407 → 1,155. The removed values were states, counties, continents and placeholders.
- Manifest field `years` renamed to `catalog_file_years` (it lists harvest-file years, not run release years).
- Discovery queries no longer exclude soil, sediment, freshwater, river, lake or "plant".

### Fixed
- NCBI requests on networks that inspect HTTPS: a certificate error now retries once with the macOS System keychain roots (still verified), `UMDB_CA_BUNDLE` adds a custom root, and persistent certificate errors stop immediately with instructions instead of retrying. `umdb backfill` / `deepest` stop when a TLS error or two consecutive failed years would make every remaining year fail.
- **Run counts were inflated by 78%.** Exports read every `srr_catalog_<YEAR>_partNNN.jsonl` file twice (the year glob matched the part files, which were then read again through the base file). The April 2026 release reported 106,558 runs; the catalog holds **59,921 unique runs**. Run-level counts (runs, assay, country, city and year tallies) were affected; BioSample and BioProject counts were not. Exports now return one base path per year and never emit a run twice.
- `backfill-year` and `daily` never returned any records: they searched SRA with `datetype=edat`, which SRA ignores, and `backfill-year` sent ISO `YYYY-MM-DD` dates, which return nothing. Both now use `pdat` (public release date) with `YYYY/MM/DD`. Until this fix only `crawl` had ever harvested data.
- esummary now covers every UID in batches of 200. It previously stopped at the first 500 of up to 2,500 per page.
- SRA experiment titles are read from `ExpXml`. Every record harvested before this fix has an empty `title`.
- "plant" in the exclusion list silently dropped wastewater treatment plant studies.
- The English word "its" (and words like "sites") no longer triggers the ITS class.
- The country fallback no longer matches aliases inside words (e.g. "uk" in "Fukuoka").

### Known issues
- Workflows call `python3 -m scripts.umdb_harvester.cli`, which exits without running. Scheduled crawls currently harvest nothing.
- AI-reviewed values override rule values even when unchanged, so older reviews can re-introduce pre-fix city values.

## 2026-04-17 — AI curation snapshot

- Published catalog: reported as 106,558 runs (actually 59,921 unique runs; see Fixed above), 54,318 BioSamples, 1,610 BioProjects.
- AI-assisted metadata review (`curate-ai`) and the full-dataset AI curation workflow.
- Analytics, global map, press kit and reviewer-facing pages.

## 2026-01 to 2026-03

- Initial harvester (SRA discovery, RunInfo flattening, BioSample/BioProject enrichment), multi-profile discovery, chunked JSON exports, BioProject explorer and dataset bundle exports.
