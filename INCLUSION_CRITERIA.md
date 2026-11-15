# How UMDB decides a study is urban

**Short answer: a broad keyword search decides what enters the catalog. A strict, sample-level eligibility screen then decides how much of it counts as urban.**

> A study that mentions a city is not necessarily an urban microbiome study. UMDB requires evidence that the **biological material** was collected from an urban-associated environment, not just that the researchers, hospital or sequencing center were in a city.

| Stage | What it does | Removes records? |
| --- | --- | --- |
| 1. Discovery | Keyword search of NCBI SRA (seven urban-environment profiles) | Decides inclusion in the broad index |
| 2. Environmental eligibility | Environmental sample, or host / clinical / laboratory material? | No, labels |
| 3. Urban-origin verification | Sample-site, infrastructure, location and study evidence of urban origin | No, labels |
| 4. Annotation | Tier, urban context category, location resolution, structured evidence | No, labels |

The core rule is **confirmed urban = E ∧ U ∧ ¬X**: evidence of an environmental sample (E), sample-specific evidence of urban origin (U), and no exclusion evidence (X). A city name supports U but never establishes E.

| Tier | Meaning | Runs | BioSamples |
| --- | --- | ---: | ---: |
| `confirmed_urban` | Environmental sample with sample-level urban evidence: explicit urban setting or wording, or named urban infrastructure (subway, WWTP, sewer network, street…) | 3,362 | 3,231 |
| `probable_urban` | Environmental sample with indirect evidence only: a city-level location, or urban/surveillance context stated at BioProject level | 17,209 | 15,578 |
| `uncertain` | Environmental origin plausible but urban association not established, no environmental evidence at all, or conflicting evidence | 18,232 | 16,611 |
| `non_urban` | Environmental sample from an explicitly non-urban site: rural, farm, forest, marine, industrial, space flight | 2,093 | 2,084 |
| `non_environmental` | Host-derived, clinical or laboratory material (stool, blood, patient isolates, cultures, bioreactors) | 19,025 | 16,816 |

*Counts are for the current catalog of 59,921 unique runs (rules version 2026.11.6).*

**Nothing is deleted.** UMDB keeps two views of the same data:

- **High-confidence urban collection:** `confirmed_urban`, or `confirmed_urban` + `probable_urban` for a broader cohort.
- **Discovery index:** everything, with each record's tier and the reason for it.

How to filter:

- In the [explorer](https://aglucaci.github.io/UMDB/explorer.html), use the **Urban eligibility** filter. Each run shows its tier and urban context; hover for the evidence.
- In downloaded data, use `eligibility.status` on each record. Tier counts are in `summary.json` → `eligibility`.

The screen is rule-based and has **not yet been benchmarked** against expert labels (see [limitations](#known-limitations)).

Policy and annotation rules: [CURATION.md](CURATION.md). Field definitions: [DATA_DICTIONARY.md](DATA_DICTIONARY.md).

---

## 1. The keyword search

### How the search is run

- **Database:** NCBI SRA, through E-utilities `esearch` (`db=sra`). Each hit is an SRA **experiment**, and all of its runs are ingested.
- **Fields searched:** organism labels via the `[ORGN]` field (NCBI's environment taxa, such as `wastewater metagenome`), plus phrases searched across all fields (titles, abstracts, sample attributes, organism names, library descriptions).
- **Matching:** NCBI's own term matching. Quoted phrases match as phrases. Word forms are spelled out (metagenome, metagenomes, metagenomic…) because NCBI truncates short wildcards such as `metagenom*` to 600 variants and drops "ITS" as a stopword.
- **Combining profiles:** a record needs to match **any one** profile. Duplicates across profiles are merged.
- **Code:** `QUERY_PROFILES` in [`scripts/umdb_harvester/config.py`](scripts/umdb_harvester/config.py). Current version: `QUERY_PROFILES_VERSION = "2026.11.2"`.

### Design principles

1. **Discovery is for recall; eligibility is for precision.** Queries find candidates, and the [eligibility screen](#3-the-eligibility-screen) decides what counts as urban.
2. **No catch-all words.** Terms such as "surface", "air", "swab", "station", "bus" or "train" appear only inside urban phrases ("public transit", "urban air"), never alone.
3. **No global NOT block.** Soil, water, sediment, plants and hospitals can all be urban. Exclusion happens per sample, in the screen.
4. **Prefer NCBI environment labels.** `wastewater metagenome`, `air metagenome`, `indoor metagenome`, `urban metagenome`, `dust metagenome`, `aerosol metagenome` and `(activated) sludge metagenome` are precise, submitter-chosen labels.

### The seven default profiles

"Method" below means any community assay: microbiome(s), microbiota, metagenome(s), metagenomic(s), metatranscriptome / metatranscriptomic(s), virome, amplicon, 16S, 16S rRNA, 18S, internal transcribed spacer, ITS1, ITS2.

| Profile | Matches | 2025 experiments |
| --- | --- | ---: |
| `municipal_wastewater` | `wastewater` / `activated sludge` / `sludge metagenome` labels, or (wastewater, sewage, sewer, influent) **and** (municipal, urban, city, treatment plant, WWTP, sewershed, community). **Excludes** SARS-CoV-2 targeted amplicon runs | 46,846 |
| `air_dust` | `air` / `aerosol` / `dust metagenome` labels, or (urban air, city air, bioaerosol(s), road dust, particulate matter) **and** method | 10,858 |
| `built_environment` | `indoor metagenome` label, or (built environment, indoor microbiome, indoor air, public building, classroom(s), office building) **and** method | 2,690 |
| `urban_metagenome` | `urban metagenome` label | 523 |
| `transit` | (subway, metro station, public transit / transport / transportation, railway / train / bus station, turnstile(s), handrail(s)) **and** method | 1,011 |
| `urban_soil_greenspace` | (urban soil(s), urban park, city park, urban green, greenspace, green space, roadside, urban garden) **and** method | 2,569 |
| `urban_water_drainage` | (stormwater, storm water, storm drain, urban river / stream / lake / canal, combined sewer, urban runoff) **and** method | 900 |
| **All seven combined** | | **64,173** |

**Opt-in profile (not in the defaults):** `wastewater_sars_cov_2` is wastewater matches **and** a SARS-CoV-2 targeted amplicon panel, about 57,800 experiments in 2025. These public-health pathogen panels are not microbiome studies. Add them with `--profile wastewater_sars_cov_2`.

Volume by year (all seven combined): 2025: 64,173 · 2020: 26,429 · 2015: 5,660. The legacy queries matched roughly 600,000+ experiments in 2025.

The exact query strings, character for character, are printed by:

```bash
umdb queries
```

### Query history

| Version | Change |
| --- | --- |
| before 2026.11 | Five broad profiles (`urban_shotgun`, `urban_amplicon`, `wastewater_surveillance`, `transit_and_surface`, `air_and_dust`) with a global NOT block that excluded soil, sediment, freshwater, river, lake and "plant" (which dropped wastewater treatment plant studies). **The current published catalog was harvested with these.** |
| 2026.11 | Those exclusions removed |
| 2026.11.2 | The seven environment profiles above. The legacy profiles are kept as `LEGACY_QUERY_PROFILES` in `config.py` for reproducibility |

## 2. What the search finds and misses

### Measured against the current catalog

For runs already in the catalog, the experiments the new queries still return, by eligibility tier:

| Tier in current catalog | Released 2024 | Released 2025 |
| --- | ---: | ---: |
| `confirmed_urban` | 167 / 170 (**98.2%**) | 526 / 609 (86.4%) |
| `probable_urban` | 8,956 / 9,432 (95.0%) | 1,716 / 2,019 (85.0%) |
| `uncertain` | 3,099 / 4,852 (63.9%) | 3,519 / 5,543 (63.5%) |
| `non_urban` | 544 / 612 (88.9%) | 73 / 819 (8.9%) |
| `non_environmental` | 25 / 4,702 (**0.5%**) | 141 / 4,236 (3.3%) |

Almost all host, clinical and laboratory material drops out, and most urban runs are kept. Of the confirmed-urban runs no longer found in 2025, the largest group was a camel-stable farm study that the screen had wrongly confirmed because of the place name "Fukang City"; the screen now ignores "X City" place names. This comparison can only measure recall on what the legacy queries found; studies neither query set finds are invisible to it, which is why an independent benchmark is planned (§ 6).

### Known gaps

| Gap | Why |
| --- | --- |
| Urban studies without an environment label or urban phrase | E.g. a surface-swab study described only as "environmental samples" with the city in the BioSample. BioSample-first discovery (planned) would find these |
| Hospital and healthcare built environments | Matched only through "built environment" / "indoor" wording |
| Non-urban material with an environment label | `air metagenome` from a rural site, industrial `wastewater metagenome`, house `dust` from a farm. The eligibility screen labels these |
| Other pathogen-panel surveillance | Influenza, RSV or mpox amplicon panels in wastewater are still in `municipal_wastewater` |

### Composition of the current catalog (harvested with the legacy queries)

Organism labels (`runinfo_row.ScientificName`) across the catalog's 59,921 unique runs. The April 2026 release reported 106,558 runs because part files were counted twice; see the [CHANGELOG](CHANGELOG.md).

| Label | Runs | Likely in scope? |
| --- | ---: | --- |
| wastewater metagenome | 17,497 | Usually |
| metagenome (unspecified) | 9,335 | Unknown |
| human gut metagenome | 4,766 | **No** |
| air metagenome | 3,834 | Usually |
| human feces metagenome | 3,638 | **No** (unless pooled sewage) |
| terrestrial metagenome | 1,572 | Depends on site |
| indoor metagenome | 1,519 | Usually |
| aerosol metagenome | 1,288 | Usually |
| *Escherichia coli* | 933 | Rarely (isolates) |
| food production metagenome | 842 | Rarely |
| *Staphylococcus aureus* | 713 | Rarely (isolates) |
| sludge / activated sludge metagenome | 1,250 | Usually |
| human skin metagenome | 578 | **No** |
| fish gut / mouse gut metagenome | 846 | **No** |

About **21% of runs (12,500)** carry a host-associated organism label (gut, feces, oral, skin, human, mouse, pig, chicken, bovine and similar). Treat that as a lower bound on out-of-scope records: unlabelled "metagenome" runs and isolates are not counted in it.

---

## 3. The eligibility screen

Code: [`scripts/umdb_harvester/eligibility.py`](scripts/umdb_harvester/eligibility.py). The screen runs at export time on every run, so rule changes apply to the whole catalog on the next `umdb export`. Each record stores the `rules_version` that produced it.

Decisions are made per **run**, from that run's own BioSample. A BioProject is never accepted wholesale: one study can contain confirmed-urban, non-urban and non-environmental samples. Study-level summaries aggregate the runs.

### Evidence it reads

| Priority | Source and field | Used for |
| --- | --- | --- |
| Critical | BioSample `isolation_source`, `env_broad_scale` / `env_biome`, `env_local_scale` / `env_feature`, `env_medium` / `env_material` | Environmental material (E), urban infrastructure and wording (U), exclusions (X) |
| Critical | BioSample `geo_loc_name` (parsed) and `lat_lon` | City-level location (supports U, probable at most); "rural" in the location → non-urban; location resolution |
| High | BioSample `building_setting`, `built_struc_set` | `urban` → explicit urban setting (confirmed); `rural` → non-urban site |
| High | BioSample `sample_type`, `*material*`, `*habitat*`, `*site*`, `description`, title | Same term lists as the critical fields |
| High | SRA organism label (`ScientificName`) | Environmental metagenome (E); host-associated, single-organism or lab labels (X) |
| High | BioProject title and description | Urban or wastewater-surveillance context (probable at most); agricultural, industrial or space-flight source (X) |
| Supporting | BioSample `host` | A real host organism is exclusion evidence. Placeholders and environment words ("Wastewater", "built environment") are ignored, since submitters often misuse the field |

The sequencing center's or submitter's location is **never** used as the collection site.

### Stage 2: environmental eligibility

- **Environmental evidence (E):** an environmental metagenome label (wastewater, sludge, air, aerosol, indoor, dust, soil, sediment, freshwater, river…), or environmental terms in BioSample fields.
- **Non-environmental (X):** host-associated metagenomes (gut, feces, oral, skin, human, mouse, plant…), single-organism labels (isolates, host genomes), clinical or host terms (stool, blood, tissue, patient, clinical, sputum, nasal…), a real host organism, or laboratory material (pure or cell culture, mock community, bioreactor, synthetic).

### Stage 3: urban-origin verification

- **Sample-specific urban evidence (U, confirmed):** explicit wording (urban, city, municipal, metropolitan, downtown), `building_setting` / `built_struc_set` = urban, or named urban infrastructure (subway, metro, transit, train or bus station, airport, turnstile, street, sidewalk, pavement, WWTP, wastewater or sewage treatment plant, sewer network, sewershed, community sewage, storm drain, city park, public building).
- **Indirect urban evidence (probable):** a parsed city in `geo_loc_name`, or BioProject text stating urban context or population-serving wastewater surveillance (wastewater/sewage surveillance, wastewater-based epidemiology, community sewage).
- **Non-urban site (X):** rural, farm, agricultural, livestock, swine, dairy, forest, marine, ocean, pristine, remote, glacier, crops, food production; space flight (ISS, spacecraft, cleanrooms); and, at BioProject level, agricultural or industrial wastewater sources (swine, slaughterhouse, aquaculture, industrial or pharmaceutical effluent, textile, mining…).

### Decision rules (first match wins)

| # | Condition | Tier |
| ---: | --- | --- |
| 1 | Laboratory or culture material | `non_environmental` |
| 2 | Host or clinical evidence, and no environmental evidence or more independent host sources (organism label, BioSample text, host field) than environmental ones | `non_environmental` |
| 3 | Host/clinical and environmental evidence in balance | `uncertain` |
| 4 | No environmental evidence | `uncertain` |
| 5 | Space flight, or a non-urban site without sample-level urban evidence | `non_urban` |
| 6 | Explicit urban setting or wording at sample level | `confirmed_urban` |
| 7 | Named urban infrastructure at sample level | `confirmed_urban` |
| 8 | BioProject names an agricultural or industrial source | `non_urban` |
| 9 | City-level location | `probable_urban` |
| 10 | Urban or wastewater-surveillance context at BioProject level | `probable_urban` |
| 11 | Otherwise | `uncertain` |

### What each record stores

```json
"eligibility": {
  "status": "confirmed_urban",
  "reasons": ["named urban infrastructure at sample level"],
  "environmental_sample": true,
  "urban_context": ["transit", "air_dust"],
  "location_resolution": "coordinate",
  "location_validation": "plausible",
  "evidence": [
    {"source": "BioSample", "field": "isolation_source", "value": "subway platform air",
     "signal": "urban_infrastructure", "terms": ["subway", "platform"]},
    {"source": "BioSample", "field": "geo_loc_name", "value": "USA: New York City",
     "signal": "urban_location"}
  ],
  "decision_method": "rules",
  "rules_version": "2026.11.6"
}
```

`urban_context` uses seven categories, tagged independently of the sequencing assay: `transit`, `built_environment`, `municipal_wastewater`, `air_dust`, `soil_greenspace`, `water_drainage`, `surfaces_infrastructure`. Among confirmed and probable runs, the primary context is municipal wastewater for 12,217 runs, air and dust for 4,564, transit for 1,617, built environment for 814, surfaces for 422, soil and greenspace for 272, and water and drainage for 270.

`location_resolution` is `coordinate`, `city`, `region`, `country` or `unknown`. `location_validation` is `plausible`, `ambiguous`, `unverified` or `missing`; nothing is `verified` until a gazetteer check exists.

### Spot checks (largest BioProjects per tier)

| Tier | Examples |
| --- | --- |
| `confirmed_urban` | New York City MTA subway (PRJNA271013), community sewage (PRJNA1207805), global urban greenspace (PRJNA937895) |
| `probable_urban` | Swiss SARS-CoV-2 wastewater surveillance (PRJEB44932), Shanghai metro (PRJNA858189: city and study level only), homes across an urbanization gradient (PRJEB25617) |
| `uncertain` | Wastewater virome atlas (PRJEB102331), CDC wastewater spike-in (PRJEB80377), *E. coli* from humans and poultry (PRJNA293225) |
| `non_urban` | Space-station surfaces (PRJEB73327), swine wastewater (PRJNA1050755), 58 industrial WWTPs (PRJNA883519) |
| `non_environmental` | Infant and parental stool (PRJEB70237), *S. aureus* from cystic fibrosis patients (PRJNA1121404), children's gut strain tracking (PRJNA1356541) |

### Known limitations

- **Keyword rules, not understanding.** Unusual wording is missed, and term lists will need tuning. Report errors with a **Metadata correction** issue.
- **No gazetteer yet.** City names are text-parsed. A parsed city gives at most `probable_urban`, and coordinates are not yet checked against urban footprints.
- **Studies that describe the site only at project level stay probable.** Shanghai metro, for example, says "metro" in its BioProject but not in each BioSample.
- **Wastewater with no stated context is `uncertain`.** Treatment plants serve both towns and industry; the catchment is rarely recorded.
- **Isolates are not environmental samples.** An isolate from urban wastewater lands in `uncertain` (conflicting evidence), not confirmed.
- **Not yet benchmarked.** Precision, recall and the share of urban soil, water, air and wastewater datasets lost to the strict criteria are unknown (see § 6).
- **Explorer filtering is per BioProject.** The explorer keeps a study when *any* of its runs is in the selected tiers. Filter on `eligibility.status` per run for cohorts.

## 4. Steps that do not filter

- **Run table, BioSample and BioProject retrieval** adds context only.
- **Geography** ([CURATION.md](CURATION.md#geography)) labels country and city. A record with no location, or a non-urban one, stays in.
- **Assay classification** labels the method only.
- **AI review** (`ai_curation.urban_origin`: `urban` / `non_urban` / `uncertain`) is stored next to the record and never removes it. **No AI reviews appear in the current published files** (`docs/db/`), although an AI curation run was recorded in April 2026.
- **Website:** the explorer's *AI urban only* option keeps BioProjects with at least one run labelled `urban`. With no published reviews it currently returns nothing. Use the **Urban eligibility** filter instead.

---

## 5. Filtering the data yourself

Use the screen's tier, and keep the evidence for your methods section:

```python
KEEP = {"confirmed_urban"}                           # add "probable_urban" for a broader cohort
urban = [r for r in records if (r.get("eligibility") or {}).get("status") in KEEP]

# Count BioSamples, not runs:
n_samples = len({r["runinfo_row"]["BioSample"] for r in urban})
```

Then **inspect the BioProject titles of what remains**: study-level review is still the most reliable check. Also:

- Use `geo.city_status == "parsed"` before relying on `geo.city`.
- Report the `eligibility.rules_version` and the UMDB release date or commit you used.

## 6. Planned improvements

1. **Benchmark:** an independently reviewed set of 200–300 BioProjects and 500–600 BioSamples across all discovery profiles, including urban, rural, clinical, laboratory and ambiguous examples plus a positive-control set of known urban studies. Two curators label a subset independently. Report precision, recall, F1, false-positive rate and the uncertain fraction, with confidence intervals, for the keyword search, the rule screen and AI review.
2. **BioSample-first discovery:** search BioProject and BioSample as well as SRA, so studies whose SRA records lack urban terms are found through their sample metadata. (The seven environment profiles without a global NOT block landed in 2026.11.2.)
3. **Geographic validation:** ISO 3166 countries, GeoNames IDs and hierarchy, a check that coordinates fall in an urban footprint, and conflict flags when city and coordinates disagree. Record the serviced catchment for wastewater when known.
4. **Reviewed overrides** for individual studies, applied on top of the rules.
5. **Separate published files** for the high-confidence urban collection and the full discovery index.

Until those land, describe UMDB as a *discovery index of public sequencing data from urban-associated contexts, with sample-level, rule-based eligibility labels*, and report the tiers and rules version you used.
