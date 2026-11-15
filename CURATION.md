# UMDB Curation Policy

How UMDB decides what is in the catalog and how each record is annotated. Read this before changing discovery queries, classifiers or AI curation, and before drawing conclusions from the data.

Field definitions are in [DATA_DICTIONARY.md](DATA_DICTIONARY.md).

## Principles

1. **Original metadata are never overwritten.** Repository values stay verbatim in `runinfo_row`, `geo.raw` and the BioSample cache. Every derived value sits next to them.
2. **Unknown beats wrong.** When evidence is weak, fields stay empty and a status or flag says why.
3. **Structured fields beat free text.** INSDC library fields and `geo_loc_name` outrank titles and descriptions.
4. **Every annotation is reproducible.** Rule-based values are re-derived from stored inputs at every export, so a rule change applies to the whole catalog, and the same inputs always give the same output.
5. **Collection place is not submitter place.** `CenterName` and the sequencing center's location are never used as the sample location.

## What is in the catalog (inclusion)

> Full detail, including the exact queries, measured false positives and filtering recipes, is in **[INCLUSION_CRITERIA.md](INCLUSION_CRITERIA.md)**.

**Current state:** a run is in UMDB if its SRA experiment matched at least one discovery query profile ([`config.py`](scripts/umdb_harvester/config.py)) at harvest time. A query match is a *candidate*, not a confirmation that the sample is urban. The catalog contains false positives, for example host-associated gut samples from studies that mention "hospital"; the eligibility screen below labels them.

Discovery profiles (version `QUERY_PROFILES_VERSION`):

| Profile | Targets |
| --- | --- |
| `municipal_wastewater` | Municipal wastewater, sewage and sludge (SARS-CoV-2 amplicon panels excluded) |
| `air_dust` | Urban and indoor air, aerosols, dust |
| `built_environment` | Buildings, classrooms, offices, indoor surfaces |
| `urban_metagenome` | Samples labelled `urban metagenome` |
| `transit` | Subways, stations, public transport |
| `urban_soil_greenspace` | Urban soil, parks, greenspace, roadsides |
| `urban_water_drainage` | Stormwater, urban rivers and canals, combined sewers |
| `wastewater_sars_cov_2` *(opt-in)* | SARS-CoV-2 targeted amplicon wastewater surveillance |

Soil, sediment, freshwater, rivers and lakes are **not** excluded: urban soils and waterways are in scope. Eligibility should depend on sampling context, not on the material.

**Eligibility tiers** are assigned to every run by the rule-based screen in [`eligibility.py`](scripts/umdb_harvester/eligibility.py) and stored as `eligibility.status`. Records are labelled, never deleted. Rules, evidence sources, spot checks and limitations: [INCLUSION_CRITERIA.md § 3](INCLUSION_CRITERIA.md#3-the-eligibility-screen).

Confirmed urban = environmental sample (E) ∧ sample-specific urban evidence (U) ∧ no exclusion (X). A city name supports U but never establishes E.

| Status | Meaning |
| --- | --- |
| `confirmed_urban` | Environmental sample with sample-level urban evidence (explicit urban setting or wording, or named urban infrastructure) |
| `probable_urban` | Environmental sample with indirect urban evidence only (city-level location, or BioProject-level urban or surveillance context) |
| `uncertain` | Urban association not established, no environmental evidence, or conflicting evidence |
| `non_urban` | Environmental sample from an explicitly non-urban site (rural, farm, forest, marine, industrial, space flight) |
| `non_environmental` | Host-derived, clinical or laboratory material |
| *withdrawn* | Planned: no longer publicly available |

A sample collected *in* a city is not automatically an *urban environmental* sample. A patient sample from an urban hospital, for instance, is out of scope.

## Annotation layers and precedence

| Layer | Where | Produced by |
| --- | --- | --- |
| 1. Original | `runinfo_row`, `geo.raw`, BioSample attributes | NCBI submitters |
| 2. Rule-derived | `geo.*`, `assay.*` | `biosample.py`, `assay.py`, deterministic |
| 3. AI-reviewed | `ai_curation.final_*` | Language model, cached per run |

The **effective** value shown on the site and counted in `summary.json` is `ai_curation.final_*` when present, otherwise the rule-derived value.

> **Known gap:** layer 3 currently wins even when the model simply repeated the old rule value. Reviews made before the 2026-11 geography fix can therefore re-introduce a state name as the city. The intended policy is that an AI value replaces a rule value only when the change is supported by cited evidence and passes validation. Until then, prefer `geo.city` together with `geo.city_status` for analyses.

## Geography

Input: BioSample `geo_loc_name` (falling back to `geographic location`, `country`, `location`).

1. Split on the first `:` into country and free text.
2. Split the free text on `,` and `;` and classify each token:
   - **US state abbreviation or name, Canadian or Chinese province, or a token containing** province / state / county / prefecture / region / district / oblast / territory / governorate / canton / department → `region`
   - **placeholder** (`city`, `multiple cities`, `missing`, `not collected`, …) → dropped, flagged
   - **continent** → dropped, flagged
   - anything else → **locality**
3. Exactly one locality → `city`, `city_status: parsed`. Several → the first is kept, `ambiguous`. None → `region_only` or `placeholder`.
4. Special case: a state name next to its own abbreviation (`New York, NY`) is read as the city.
5. Countries are normalized through a small alias table (`USA` → `United States`). Missing-value strings become empty.
6. If BioSample gives no country, a country alias found as a whole word in the title or sample name is used, with `country_source: text_fallback`.

**Limitations.** No gazetteer: a locality token can be a campus, park, hospital or neighbourhood, and admin units outside the US, Canada and China (e.g. Indian states) can still appear as cities. GeoNames IDs and validity flags are planned.

## Assay classes

Library fields declared at submission are used first. Free text is consulted only when they are uninformative.

| Class | Rule (first match wins) |
| --- | --- |
| `16S` / `18S` / `ITS` / `Amplicon` | `LibraryStrategy = AMPLICON` (non-viral); marker from `16S`, `18S`, case-sensitive `ITS`/`ITS1`/`ITS2`, or "internal transcribed spacer" in title, library name or attributes |
| `Viral RNA` | `LibrarySource = VIRAL RNA` (e.g. SARS-CoV-2 tiled amplicons from wastewater) |
| `Metatranscriptomics` | `LibrarySource = METATRANSCRIPTOMIC`, or an RNA strategy with "metatranscriptom*" in the text |
| `RNA-seq` | `LibrarySource = TRANSCRIPTOMIC` or an RNA strategy (`RNA-Seq`, `miRNA-Seq`, …) |
| `WGS` | Shotgun DNA strategy (`WGS`, `WGA`, `WCS`, `Targeted-Capture`, `Hi-C`, …); tag records the specific strategy |
| fallbacks | `OTHER` / missing strategy: METAGENOMIC with random selection → `WGS` (medium); PCR selection or "amplicon" text → marker/`Amplicon` (medium); text hints → `Metatranscriptomics` / `WGS` (medium) |
| `Unknown` | Nothing informative |

`confidence` is `high` when the class comes from `LibraryStrategy` or `LibrarySource`, `medium` when it comes from text or selection.

## AI-assisted review

`curate-ai` (or `--ai-curate`) sends each run's metadata (run fields, BioSample attributes, BioProject title and abstract, and the rule annotations) to a language model. The model returns sufficiency, urban origin, environment type, corrected country/city/assay and evidence strings. Prompt and output schema are in [`ai_curation.py`](scripts/umdb_harvester/ai_curation.py).

- Reviews are cached per run and re-run only when the input context changes, or with `--overwrite`.
- `confidence` is the model's own label, **not a calibrated probability**.
- Evidence strings are not yet checked against the source metadata, and there is no human-review queue.
- AI review has **not yet been benchmarked** against expert labels. Do not report AI-derived counts as validated.

Planned: a gold-standard set (~250 BioProjects / ~600 BioSamples, dual expert annotation), precision/recall and abstention metrics, evidence checks, and an acceptance policy deciding which AI changes are published.

## Reporting and correcting errors

Open a **Metadata correction** issue with:

- the run (SRR) or BioSample (SAMN) accession
- the field that is wrong and the value you believe is correct
- the evidence: a quote from the BioSample, BioProject or publication, with link or DOI

Systematic errors (a whole pattern misparsed) are fixed in the rules with a test. One-off errors will go into a reviewed override file (planned). The raw catalog is never hand-edited.

## Using UMDB data responsibly

- Count **BioSamples**, not runs, when comparing places or environments.
- Filter on `geo.city_status == "parsed"` (or check `raw`) before treating `city` as a real city.
- Do not compare microbial diversity across studies from metadata alone. Extraction kits, primers, library preparation and sequencing depth differ between studies and confound such comparisons.
- Cite the release date or commit you used (see [CITATION.cff](CITATION.cff)), since the catalog changes daily.
