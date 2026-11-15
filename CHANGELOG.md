# Changelog

User-visible changes to the UMDB pipeline, data schema and website. Data refreshes from scheduled crawls are not listed individually.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Tagged releases will follow semantic versioning once versioned database releases begin (see [MAINTAINING.md](MAINTAINING.md#releases)).

## [Unreleased]

### Added
- Deepest-search mode: `umdb deepest` (= `scripts/deep_backfill.sh --deepest`) harvests 2007 to now year by year with every profile, paging through every hit per day (`--max-per-day 0`, now the default). It re-fetches records already held (`--refresh` on `crawl`, `daily` and `backfill-year`) and compacts the catalog after each year (`umdb compact`), keeping only the newest record per run. Exports publish only the newest record per run. Refresh and compaction ask before overwriting uncommitted data.
- `umdb` command (`bin/umdb`, install with `bin/umdb install`): `umdb crawl|daily|backfill-year|curate-ai` (or `umdb run …`), `umdb backfill`, `export`, `serve`, `status`, `queries`, `test`.
- Sample-level urban eligibility screen (`eligibility.py`), following a staged model: environmental eligibility, then urban-origin verification, then annotation. Every run gets `eligibility.status` (`confirmed_urban` / `probable_urban` / `uncertain` / `non_urban` / `non_environmental`), an urban context category, location resolution, and structured evidence (source, field, value, signal). Uses `building_setting` and `built_struc_set` when present. A city name alone gives at most `probable_urban`. Tier counts are in `summary.json`, and the explorer has an **Urban eligibility** filter and a per-run label. Current catalog: 3,381 confirmed, 17,209 probable, 18,232 uncertain, 2,074 non-urban and 19,025 non-environmental runs. No records are removed.
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
- Renamed the harvester package `scripts/urbanscope_harvester` → `scripts/umdb_harvester`, the workflows to `umdb_*.yml`, and the NCBI tool name to `umdb-srr-harvester`. Older harvested records still contain `tool=urbanscope-srr-harvester` in their stored request URLs.
- Exports now re-derive `geo` and `assay` for every record from cached metadata, so rule fixes apply to the whole catalog.
- Assay classification trusts `LibraryStrategy` and `LibrarySource` before free text.
- Geography parsing classifies tokens instead of taking the last comma-separated token as the city. Effect on the catalog's 59,921 unique runs: runs with a country and city 26,209 → 20,128; distinct city labels 1,407 → 1,155. The removed values were states, counties, continents and placeholders.
- Manifest field `years` renamed to `catalog_file_years` (it lists harvest-file years, not run release years).
- Discovery queries no longer exclude soil, sediment, freshwater, river, lake or "plant".

### Fixed
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
