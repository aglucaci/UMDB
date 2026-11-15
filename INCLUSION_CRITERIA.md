# How UMDB decides a study is urban

**Short answer: a broad keyword search decides what enters the catalog. A strict, sample-level eligibility screen then decides how much of it counts as urban.**

> A study that mentions a city is not necessarily an urban microbiome study. UMDB requires evidence that the **biological material** was collected from an urban-associated environment, not just that the researchers, hospital or sequencing center were in a city.

| Stage | What it does | Removes records? |
| --- | --- | --- |
| 1. Discovery | Keyword search of NCBI SRA (five query profiles) | Decides inclusion in the broad index |
| 2. Environmental eligibility | Environmental sample, or host / clinical / laboratory material? | No, labels |
| 3. Urban-origin verification | Sample-site, infrastructure, location and study evidence of urban origin | No, labels |
| 4. Annotation | Tier, urban context category, location resolution, structured evidence | No, labels |

The core rule is **confirmed urban = E ∧ U ∧ ¬X**: evidence of an environmental sample (E), sample-specific evidence of urban origin (U), and no exclusion evidence (X). A city name supports U but never establishes E.

| Tier | Meaning | Runs | BioSamples |
| --- | --- | ---: | ---: |
| `confirmed_urban` | Environmental sample with sample-level urban evidence: explicit urban setting or wording, or named urban infrastructure (subway, WWTP, sewer network, street…) | 3,381 | 3,250 |
| `probable_urban` | Environmental sample with indirect evidence only: a city-level location, or urban/surveillance context stated at BioProject level | 17,209 | 15,578 |
| `uncertain` | Environmental origin plausible but urban association not established, no environmental evidence at all, or conflicting evidence | 18,232 | 16,610 |
| `non_urban` | Environmental sample from an explicitly non-urban site: rural, farm, forest, marine, industrial, space flight | 2,074 | 2,065 |
| `non_environmental` | Host-derived, clinical or laboratory material (stool, blood, patient isolates, cultures, bioreactors) | 19,025 | 16,815 |

*Counts are for the current catalog of 59,921 unique runs (rules version 2026.11.5).*

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

- **Database:** NCBI SRA, through E-utilities `esearch` (`db=sra`). Each hit is an SRA **experiment**; all of its runs are ingested.
- **Fields searched:** the queries use no field tags, so NCBI searches **All Fields**: experiment and study titles and abstracts, sample attributes, organism names, library descriptions, and submitter or organisation names.
- **Matching:** NCBI's own term matching. Quoted phrases match as phrases; `*` is a prefix wildcard (`metagenom*` matches metagenome, metagenomic, metagenomics).
- **Combining profiles:** a record needs to match **any one** profile (logical OR across profiles). Duplicates across profiles are merged.
- **Code:** `QUERY_PROFILES` in [`scripts/umdb_harvester/config.py`](scripts/umdb_harvester/config.py). Current version: `QUERY_PROFILES_VERSION = "2026.11"`.

### The five profiles

Each profile is a boolean expression made of term groups: **all** groups joined by AND must match, and **none** of the NOT terms may appear.

#### `urban_shotgun`: urban shotgun metagenomics and metatranscriptomics

| Group | A record must mention at least one of… |
| --- | --- |
| Place | urban, city, cities, metropolitan, municipal, built environment, subway, metro, transit, railway, airport, station, public transit/transport, wastewater, sewage, sewer, stormwater, street, sidewalk, pavement, road, building(s), housing, dormitory, hospital, school, campus, surface swab, fomite, surface, air, aerosol, dust |
| **AND** Method | whole genome shotgun, shotgun metagenom\*, shotgun sequencing, metagenom\*, metatranscriptom\*, total RNA sequencing, metatranscriptome sequencing, microbiome, microbiota |
| **AND** Environment | environment\*, built environment, wastewater, sewage, sewer, stormwater, surface, swab, air, aerosol, dust |
| **NOT** | single-cell, scRNA, whole exome, WES, marine, ocean, forest, agricultur\*, farm, rhizosphere |

#### `urban_amplicon`: urban marker-gene studies

Same Place and Environment groups as `urban_shotgun` (plus `swab` in Place), and the same NOT list.

| Group | A record must mention at least one of… |
| --- | --- |
| **AND** Method | amplicon, marker gene, 16S, 16S rRNA, 18S, ITS, V3-V4, V4 region, internal transcribed spacer, microbiome, microbiota |

#### `wastewater_surveillance`: wastewater and sewage

| Group | A record must mention at least one of… |
| --- | --- |
| Sample | wastewater, sewage, sewer, stormwater, influent, effluent, sludge, waste water, waste-water |
| **AND** Setting | urban, city, cities, metropolitan, municipal, community, public |
| **AND** Method | microbiome, microbiota, metagenom\*, metatranscriptom\*, amplicon, 16S, ITS, virome |
| NOT | *(none)* |

#### `transit_and_surface`: transit systems and touched surfaces

| Group | A record must mention at least one of… |
| --- | --- |
| Place/sample | subway, metro, transit, railway, train, bus, airport, terminal, station, public transit/transport, surface, swab, fomite, handrail, turnstile, bench, kiosk |
| **AND** Method | microbiome, microbiota, metagenom\*, metatranscriptom\*, amplicon, 16S, ITS |
| NOT | *(none)* |

> ⚠️ **This profile has no urban term.** "swab AND microbiome" is enough to match, so human skin, oral and vaginal swab studies qualify.

#### `air_and_dust`: urban and indoor air

| Group | A record must mention at least one of… |
| --- | --- |
| Setting | urban, city, cities, metropolitan, municipal, built environment, building, hospital, school, classroom, office |
| **AND** Sample | air, aerosol, bioaerosol, dust, particulate |
| **AND** Method | microbiome, microbiota, metagenom\*, metatranscriptom\*, amplicon, 16S, ITS |
| NOT | *(none)* |

The exact query strings, character for character, are printed by:

```bash
cd /Users/alex/Documents/UMDB && PYTHONPATH=scripts python3 -c "from umdb_harvester.config import QUERY_PROFILES as Q; [print(k, v, sep='\n', end='\n\n') for k, v in Q.items()]"
```

### Query history

| Version | Change |
| --- | --- |
| before 2026.11 | `urban_shotgun` and `urban_amplicon` also excluded soil, sediment, freshwater, river, lake and plant. "plant" dropped wastewater treatment plant studies. **The current published catalog was harvested with these older queries.** |
| 2026.11 | Those exclusions removed. Urban soils and waterways are in scope; eligibility should depend on sampling context, not material |

---

## 2. What the search lets through

Keyword matching over All Fields is broad by design (high recall). Known ways non-urban records get in:

| Leak | Why |
| --- | --- |
| Human and animal microbiome studies | `transit_and_surface` needs only "swab" + "microbiome". "hospital", "school" and "building" match clinical and cohort studies |
| Clinical bacterial isolates (*E. coli*, *S. aureus*, …) | "hospital" plus "surface" or "environment" in isolate metadata |
| Institution names | "city", "metro" and "campus" match organisation names (e.g. a university with "City" in its name) |
| Generic words | "surface", "road", "station" and "air" occur in many non-urban contexts (surface water, field station) |
| Non-urban wastewater | farm or industrial wastewater mentioning "community" or "public" |

### Measured composition of the current catalog

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
  "rules_version": "2026.11.5"
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
2. **Broader discovery:** search BioProject and BioSample as well as SRA (BioSample-first finds studies whose titles lack urban terms). Revise the profiles toward seven urban environments, and drop global NOT blocks.
3. **Geographic validation:** ISO 3166 countries, GeoNames IDs and hierarchy, a check that coordinates fall in an urban footprint, and conflict flags when city and coordinates disagree. Record the serviced catchment for wastewater when known.
4. **Reviewed overrides** for individual studies, applied on top of the rules.
5. **Separate published files** for the high-confidence urban collection and the full discovery index.

Until those land, describe UMDB as a *discovery index of public sequencing data from urban-associated contexts, with sample-level, rule-based eligibility labels*, and report the tiers and rules version you used.
