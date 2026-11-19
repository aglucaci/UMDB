# UMDB roadmap

UMDB's aim is to be the global discovery and analytics platform for urban microbiome research: a researcher should get from a scientific question to an analysis-ready cohort without writing code, and a computational biologist should be able to do the same programmatically. The Python harvester, the SQLite database and the Parquet releases stay the foundation; the website and releases are built on top of them.

## Products

| Product | What it is | Status |
| --- | --- | --- |
| **Catalog** | Harmonized studies, BioSamples and runs: Explorer plus a persistent page for every study, sample, run, city, country and habitat | v1.0, implemented |
| **Atlas** | Global map: sample and study density, habitats, per-capita coverage with surveillance gaps, location confidence, collection-year filter, export of the visible area | v1.0, implemented |
| **Analytics** | Coverage, growth, assays, study concentration (Lorenz), collection-to-release delay, library sizes, metadata completeness; every chart downloadable (CSV, SVG, PNG) | v1.0, implemented |
| **Workspace** | Browser-local cohorts: save from the Explorer, union/intersection/difference, comparison with imbalance flags, seeded balancing, research-package export | v1.0, implemented |
| **Collections** | Curated, citable cohorts with inclusion criteria and run lists: global wastewater, transit, urban air, greenspace, virome, resistome | v1.0, implemented |
| **Profiles** | Standardized taxonomic and functional abundance profiles from uniformly reprocessed shotgun data (UMDB-SCIENCE pipeline, Track B) | v2.0 |

Long-term: UMDB 1.0 discovery → 2.0 biology (species, genes, pathways, genomes, viruses) → 3.0 comparative ecological and evolutionary analyses → 4.0 AI-assisted hypothesis generation and prediction.

## Routes

| Entity | Page | Short route (via `404.html` on GitHub Pages) |
| --- | --- | --- |
| BioProject | `study.html?acc=PRJNA271013` | `/study/PRJNA271013` |
| BioSample | `sample.html?acc=SAMN03270027` | `/sample/SAMN03270027` |
| Run | `run.html?acc=SRR1748537` | `/run/SRR1748537` |
| City | `city.html?id=5128581` (GeoNames id) | `/city/5128581` |
| Country | `country.html?iso=US` | `/country/US` |
| Habitat | `habitat.html?id=municipal-wastewater` | `/habitat/municipal-wastewater` |
| Collection | `collections.html?slug=global-wastewater` | `/collection/global-wastewater` |
| Taxon | | v2.0 |

Every filter state is in the page address, so any Explorer, Atlas or record view can be bookmarked or shared.

## Design decisions

* **Static first.** The site is static files with no build step and no server. Data generation (Python), storage (SQLite, Parquet, JSON) and presentation (browser) are separate; the browser never needs a running backend.
* **Exact counts from small indexes.** The Explorer, Atlas, entity pages and Workspace share one query engine (`docs/assets/db.js`) over two cubes in which every BioSample and run is counted exactly once. Details load per project on demand.
* **Reproducible cohorts.** A cohort is a JSON definition (filters or set operations, balancing, seed) tied to a database build. The browser and `umdb cohort export` implement the same filters and the same seeded shuffle; a test checks the PRNG against the browser's output, and on the real catalog both select identical samples.
* **Honest maps.** Points are submitter-reported coordinates (rounded to about 1 km), never city centroids inferred from names; a validated city is drawn only when selected. A location-confidence layer shows where samples are mapped at country level only.
* **Licensing.** UMDB's curation and derived assets are CC BY-NC-SA 4.0; sequence data and submitter metadata stay under the INSDC policies of their archives. Releases record both (`manifest.json` → `licenses`).

## Not done yet

| Item | Why it matters | Notes |
| --- | --- | --- |
| Independent eligibility benchmark | Required before tagging v1.0.0 | UMDB-SCIENCE C8 writes a blinded, stratified labeling sheet (500 BioSamples); needs two expert raters |
| Fix invalid collection dates in the catalog | 180 BioSamples have future, post-release or pre-1950 dates | Flagged on sample pages and excluded from temporal analyses; not yet corrected or nulled in the database |
| Hosting split | The site plus `docs/db/` is about 306 MB in git, close to GitHub Pages' 1 GB guidance | Keep the pages on GitHub Pages or Cloudflare Pages; move releases and `docs/db/p/` to object storage (Cloudflare R2: free egress, HTTP range requests for Parquet). Needs an account and a decision |
| DuckDB-Wasm queries on Parquet | Arbitrary record-level queries in the browser | Possible once releases are served with range requests and CORS |
| React/TypeScript port | Larger UI (virtualized tables, drawing selection areas on the map) | The current pages are framework-free; the data files and engine are designed so a port can reuse them |
| Drawn geographic selections | Select samples by polygon | The Atlas exports the visible area today |
| More enrichment | WorldPop, GHSL, ERA5, OpenAQ, OpenStreetMap, WHO/UNICEF JMP, ENVO mapping, Europe PMC/Crossref publications | GeoNames population and World Bank WDI are integrated (World Bank in the research pipeline); each new layer must record source, version, resolution and method |
| Accounts and shared workspaces | Cohorts that follow a user across devices | Not planned before v2.0; cohort.json files can be shared today |

## Performance targets (to benchmark, not measured guarantees)

| Metric | Target | Today |
| --- | --- | --- |
| Initial compressed data index | under 1 MB | About 1.1 MB gzipped for the five index files (`projects.json` is 0.44 MB of it; it could load lazily) |
| Warm filter update | under 300 ms | One pass over about 57,000 cube rows |
| Search result retrieval | under 1 s | Accession lookup reads one index file of about 160 KB |
| Initial map render | under 3 s | Depends on the OpenFreeMap basemap |
| Account required for public data | No | No |
| Mobile and tablet | Required | Pages are responsive; tables scroll horizontally |

## Before tagging v1.0.0

- [ ] Eligibility benchmark completed (C8 labels) and reported in the manuscript
- [ ] Invalid collection dates corrected or nulled, with a changelog entry
- [ ] `umdb release --version 1.0.0` built and verified (`shasum -a 256 -c checksums.sha256`)
- [ ] Release deposited in Zenodo; DOI added to `CITATION.cff` and the Data page
- [ ] Download links tested from a clean browser profile
- [ ] Hosting decision for releases and project files (GitHub Releases or R2)
