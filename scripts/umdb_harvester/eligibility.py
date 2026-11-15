"""Rule-based urban eligibility screen.

Every run is assessed in three stages after keyword discovery:

  Stage 2  Environmental eligibility: is this an environmental sample, or
           host / clinical / laboratory material?            (E, not X_env)
  Stage 3  Urban-origin verification: is there evidence the material was
           collected in an urban-associated environment?      (U, not X_site)
  Stage 4  Tier and context: one tier, the urban context category, location
           resolution, and every piece of evidence behind the decision.

Tiers (in rule order):

  non_environmental  host-derived, clinical, laboratory/culture material
  uncertain          no environmental-sample evidence, or conflicting evidence
  non_urban          environmental, but from an explicitly non-urban site
                     (rural, farm, forest, marine, space flight, industrial)
  confirmed_urban    environmental AND sample-specific urban evidence: explicit
                     urban setting/wording, or named urban infrastructure
                     (subway, WWTP, municipal sewer network, street ...)
  probable_urban     environmental AND indirect urban evidence: a parsed city
                     name, or urban context stated only at BioProject level
  uncertain          environmental, urban association not established

Eligible (confirmed) = E and U and not X. A city name supports U but never
establishes E. Records are labelled, never deleted. "Sample level" means the
SRA organism label and BioSample fields; BioProject text describes a whole
study and only ever supports probable_urban.

Bump RULES_VERSION whenever a term list or decision rule changes.
"""
from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

RULES_VERSION = "2026.11.5"

TIERS = ("confirmed_urban", "probable_urban", "uncertain", "non_urban", "non_environmental")


def _rx(terms: Iterable[str]) -> re.Pattern:
    return re.compile(r"\b(?:" + "|".join(terms) + r")\b", re.I)


# --- Urban context categories (independent of sequencing assay) --------------
CONTEXT_TERMS: Dict[str, List[str]] = {
    "transit": [r"subways?", r"metro", r"transit", r"railways?", r"train stations?", r"trains?",
                r"bus stations?", r"buses", r"airports?", r"turnstiles?", r"platforms?",
                r"public transport(?:ation)?", r"metro stations?", r"subway stations?"],
    "municipal_wastewater": [r"waste\s?-?water", r"sewage", r"sewers?", r"sewershed", r"wwtps?",
                             r"(?:waste\s?-?water|sewage) treatment plants?", r"treatment plants?",
                             r"influent", r"activated sludge", r"sludge"],
    "built_environment": [r"built[ -]environment", r"indoor", r"buildings?", r"offices?", r"classrooms?",
                          r"dormitor(?:y|ies)", r"homes?", r"houses?", r"apartments?", r"hvac",
                          r"hospital (?:surfaces?|sinks?|floors?|drains?|air)", r"floors?",
                          r"door ?(?:handles?|knobs?)", r"public (?:buildings?|facilit(?:y|ies))"],
    "air_dust": [r"air", r"aerosols?", r"bioaerosols?", r"dust", r"particulate(?: matter)?",
                 r"pm2\.5", r"pm10", r"ambient air", r"outdoor air", r"indoor air"],
    "soil_greenspace": [r"soils?", r"parks?", r"greenspaces?", r"green spaces?", r"gardens?",
                        r"street trees?", r"roadside", r"lawns?", r"turf"],
    "water_drainage": [r"stormwater", r"storm drains?", r"drainage", r"canals?", r"rivers?", r"lakes?",
                       r"ponds?", r"freshwater", r"combined sewer overflows?", r"runoff"],
    "surfaces_infrastructure": [r"sidewalks?", r"pavements?", r"streets?", r"roads?", r"benches",
                                r"bench", r"handrails?", r"kiosks?", r"fomites?", r"surfaces?",
                                r"atms?", r"railings?"],
}
CONTEXT_RX = {k: _rx(v) for k, v in CONTEXT_TERMS.items()}
ANY_ENV_RX = _rx([t for v in CONTEXT_TERMS.values() for t in v]
                 + [r"sediments?", r"biofilms?", r"groundwater", r"environment(?:al)? samples?", r"swabs?"])

# Explicit urban wording, or infrastructure that only exists in urban systems.
URBAN_EXPLICIT = _rx([r"urban", r"city", r"cities", r"municipal", r"metropolitan", r"downtown",
                      r"megacity", r"inner[ -]city"])
URBAN_INFRASTRUCTURE = _rx([
    r"subways?", r"metro", r"transit", r"public transport(?:ation)?", r"train stations?",
    r"bus stations?", r"metro stations?", r"subway stations?", r"airports?", r"turnstiles?",
    r"streets?", r"sidewalks?", r"pavements?", r"wwtps?", r"(?:waste\s?-?water|sewage) treatment plants?",
    r"sewer (?:systems?|networks?)", r"sewershed", r"community sewage", r"combined sewer",
    r"storm drains?", r"city parks?", r"public (?:buildings?|facilit(?:y|ies))",
])
# Study-level wording for population-serving wastewater surveillance. Sewage
# collected for community surveillance is urban-associated even when the
# treatment plant lies outside city limits, so it supports probable_urban.
PROJECT_URBAN_SURVEILLANCE = _rx([
    r"waste\s?-?water surveillance", r"sewage surveillance", r"waste\s?-?water[ -]based epidemiology",
    r"wbe", r"community (?:waste\s?-?water|sewage)", r"public health surveillance",
    r"waste\s?-?water monitoring", r"sewage monitoring",
])
# Organism labels for environmental samples (sample-level E).
ORG_ENV = _rx([r"wastewater", r"sewage", r"sludge", r"air", r"aerosol", r"indoor", r"built environment",
               r"urban", r"dust", r"stormwater", r"soil", r"terrestrial", r"sediment", r"freshwater",
               r"river", r"lake", r"pond", r"groundwater", r"biofilm", r"anaerobic digester", r"compost"])

# Stage 2 exclusions: host-derived, clinical, laboratory material (X_env).
ORG_HOST = _rx([r"gut", r"feces", r"fecal", r"faecal", r"oral", r"skin", r"vaginal", r"lung",
                r"respiratory tract", r"nasopharyngeal", r"blood", r"milk", r"rumen", r"insect",
                r"mosquito", r"human", r"mouse", r"rat", r"pig", r"bovine", r"chicken", r"fish",
                r"plant", r"root", r"rhizosphere", r"phyllosphere", r"leaf", r"homo sapiens", r"mus musculus"])
ORG_LAB = _rx([r"bioreactor", r"synthetic", r"mock", r"food fermentation"])
SAMPLE_NONENV = _rx([
    r"feces", r"faeces", r"fecal", r"faecal", r"stool", r"gut", r"intestin\w*", r"colon", r"rectal",
    r"cecum", r"caecum", r"oral", r"saliva", r"sputum", r"dental", r"tongue", r"vaginal", r"nasal",
    r"nasopharyngeal", r"oropharyngeal", r"throat", r"blood", r"serum", r"plasma", r"tissue",
    r"biops(?:y|ies)", r"patients?", r"clinical", r"skin swabs?", r"skin surface", r"rumen", r"milk",
])
LAB = _rx([r"cell culture", r"pure culture", r"mock community", r"bioreactors?", r"laboratory culture",
           r"enrichment culture"])

# Stage 3 exclusions: environmental but explicitly non-urban site (X_site).
NONURBAN_SITE = _rx([
    r"rural", r"farms?", r"agricultur\w*", r"livestock", r"poultry", r"swine", r"piggery", r"cattle",
    r"dairy", r"manure", r"forests?", r"marine", r"seawater", r"ocean", r"coral", r"pristine",
    r"remote", r"wilderness", r"glacier", r"permafrost", r"rhizosphere", r"crops?", r"vegetables?",
    r"food production", r"food processing",
])
RURAL = _rx([r"rural", r"countryside"])
SPACE = _rx([r"iss", r"international space station", r"spacecraft", r"space station", r"clean ?rooms?"])
PROJECT_NONURBAN = _rx([
    r"swine", r"pig farms?", r"piggery", r"livestock", r"poultry", r"dairy", r"cattle",
    r"slaughterhouses?", r"abattoirs?", r"aquaculture", r"industrial (?:waste\s?-?water|effluent)",
    r"pharmaceutical (?:waste\s?-?water|effluent)", r"textile", r"paper mill", r"coking",
    r"landfill leachate", r"mine drainage", r"mining",
])
HOSPITAL = _rx([r"hospital"])

ENV_ATTR_HINTS = ("env", "source", "sample_type", "sample type", "material", "habitat", "feature",
                  "biome", "metagenome_source", "body", "tissue", "building_setting", "built_struc",
                  "setting", "site", "description")
SETTING_KEYS = ("building_setting", "built_struc_set", "building setting", "built structure setting")
HOST_KEYS = ("host", "host scientific name", "host_scientific_name", "host common name", "host_common_name")
HOST_PLACEHOLDERS = {
    "", "not applicable", "none", "missing", "not collected", "na", "n/a", "not provided",
    "environment", "environmental", "unknown", "null", "not available",
}
SKIP_KEYS = ("geo_loc", "lat_lon", "collected_by", "sequenced_by", "project_name", "collection_date")


def _ev(source: str, field: str, value: str, signal: str, rx: Optional[re.Pattern] = None) -> Dict[str, Any]:
    item: Dict[str, Any] = {"source": source, "field": field, "value": value[:200], "signal": signal}
    if rx is not None:
        terms: List[str] = []
        for m in rx.finditer(value):
            t = m.group(0).lower()
            if t not in terms:
                terms.append(t)
        item["terms"] = terms[:6]
    return item


def _sample_fields(biosample: Dict[str, Any]) -> List[Tuple[str, str]]:
    attrs = biosample.get("attributes", {}) if isinstance(biosample, dict) else {}
    out: List[Tuple[str, str]] = []
    for key, val in (attrs or {}).items():
        k = key.lower()
        if any(k.startswith(s) for s in SKIP_KEYS) or k in HOST_KEYS:
            continue
        if any(h in k for h in ENV_ATTR_HINTS) and str(val).strip():
            out.append((key, str(val).strip()))
    title = (biosample.get("title") or "").strip() if isinstance(biosample, dict) else ""
    if title:
        out.append(("title", title))
    return out


def _host(biosample: Dict[str, Any]) -> Tuple[str, str]:
    attrs = biosample.get("attributes", {}) if isinstance(biosample, dict) else {}
    for key in HOST_KEYS:
        val = str((attrs or {}).get(key, "")).strip()
        if not val or val.lower() in HOST_PLACEHOLDERS:
            continue
        # Submitters often put the environment in "host" ("Wastewater", "built environment").
        if ANY_ENV_RX.search(val) or "environment" in val.lower():
            continue
        return key, val
    return "", ""


def _location(geo: Dict[str, Any]) -> Tuple[str, str]:
    city = (geo.get("city") or "").strip()
    status = geo.get("city_status", "parsed" if city else "missing")
    if geo.get("lat") and geo.get("lon"):
        resolution = "coordinate"
    elif city and status == "parsed":
        resolution = "city"
    elif geo.get("region"):
        resolution = "region"
    elif geo.get("country"):
        resolution = "country"
    else:
        resolution = "unknown"
    # No gazetteer yet, so nothing can be "verified" or "conflicting".
    if status == "ambiguous":
        validation = "ambiguous"
    elif resolution in ("coordinate", "city"):
        validation = "plausible"
    elif resolution == "unknown":
        validation = "missing"
    else:
        validation = "unverified"
    return resolution, validation


def assess_eligibility(
    rec: Dict[str, Any],
    biosample: Dict[str, Any] | None = None,
    bioproject: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    biosample = biosample if isinstance(biosample, dict) else {}
    bioproject = bioproject if isinstance(bioproject, dict) else (rec.get("bioproject") or {})
    row = rec.get("runinfo_row") or {}
    geo = rec.get("geo") or {}

    organism = (row.get("ScientificName") or "").strip()
    org_low = organism.lower()
    host_key, host_val = _host(biosample)
    project = [(k, (bioproject.get(k) or "").strip()) for k in ("title", "description")]
    project = [(k, v) for k, v in project if v]

    evidence: List[Dict[str, Any]] = []
    env_src: set = set()
    nonenv_src: set = set()
    lab: List[str] = []
    site_neg: List[str] = []
    explicit: List[str] = []
    infra: List[str] = []
    contexts: Dict[str, int] = {}

    # --- Organism label (sample level) ---
    if organism:
        if "metagenome" not in org_low:
            evidence.append(_ev("SRA", "ScientificName", organism, "non_environmental"))
            nonenv_src.add("organism")
        elif ORG_LAB.search(org_low):
            evidence.append(_ev("SRA", "ScientificName", organism, "laboratory", ORG_LAB))
            lab.append("organism")
        elif ORG_HOST.search(org_low):
            evidence.append(_ev("SRA", "ScientificName", organism, "non_environmental", ORG_HOST))
            nonenv_src.add("organism")
        elif ORG_ENV.search(org_low):
            evidence.append(_ev("SRA", "ScientificName", organism, "environmental", ORG_ENV))
            env_src.add("organism")
            for cat, rx in CONTEXT_RX.items():
                if rx.search(org_low):
                    contexts[cat] = contexts.get(cat, 0) + 2
        elif NONURBAN_SITE.search(org_low):
            evidence.append(_ev("SRA", "ScientificName", organism, "non_urban_site", NONURBAN_SITE))
            env_src.add("organism")
            site_neg.append("organism")

    # --- BioSample fields (sample level) ---
    for key, val in _sample_fields(biosample):
        if key.lower() in SETTING_KEYS:
            low = val.lower()
            if "urban" in low:
                evidence.append(_ev("BioSample", key, val, "urban_explicit"))
                explicit.append(key)
            elif "rural" in low:
                evidence.append(_ev("BioSample", key, val, "non_urban_site"))
                site_neg.append(key)
            continue
        if LAB.search(val):
            evidence.append(_ev("BioSample", key, val, "laboratory", LAB))
            lab.append(key)
        if SAMPLE_NONENV.search(val):
            evidence.append(_ev("BioSample", key, val, "non_environmental", SAMPLE_NONENV))
            nonenv_src.add("biosample")
        elif HOSPITAL.search(val) and not ANY_ENV_RX.search(val):
            evidence.append(_ev("BioSample", key, val, "non_environmental", HOSPITAL))
            nonenv_src.add("biosample")
        if ANY_ENV_RX.search(val):
            evidence.append(_ev("BioSample", key, val, "environmental", ANY_ENV_RX))
            env_src.add("biosample")
            for cat, rx in CONTEXT_RX.items():
                if rx.search(val):
                    contexts[cat] = contexts.get(cat, 0) + 2
        if URBAN_EXPLICIT.search(val):
            evidence.append(_ev("BioSample", key, val, "urban_explicit", URBAN_EXPLICIT))
            explicit.append(key)
        if URBAN_INFRASTRUCTURE.search(val):
            evidence.append(_ev("BioSample", key, val, "urban_infrastructure", URBAN_INFRASTRUCTURE))
            infra.append(key)
        if NONURBAN_SITE.search(val):
            evidence.append(_ev("BioSample", key, val, "non_urban_site", NONURBAN_SITE))
            site_neg.append(key)
        if SPACE.search(val):
            evidence.append(_ev("BioSample", key, val, "non_urban_site", SPACE))
            site_neg.append(key)
    if host_val:
        evidence.append(_ev("BioSample", host_key, host_val, "non_environmental"))
        nonenv_src.add("host")

    # --- Geography: supports U, never E ---
    city = (geo.get("city") or "").strip()
    plausible_city = bool(city) and geo.get("city_status", "parsed") == "parsed"
    raw_geo = (geo.get("raw") or "").strip()
    if RURAL.search(raw_geo):
        evidence.append(_ev("BioSample", geo.get("raw_field") or "geo_loc_name", raw_geo, "non_urban_site", RURAL))
        site_neg.append("geo_loc_name")
        plausible_city = False
    if plausible_city:
        evidence.append(_ev("BioSample", geo.get("raw_field") or "geo_loc_name", raw_geo or city, "urban_location"))

    # --- BioProject: study level, supports probable_urban at most ---
    proj_urban = proj_space = proj_nonurban = False
    for key, val in project:
        if URBAN_EXPLICIT.search(val) or URBAN_INFRASTRUCTURE.search(val) or PROJECT_URBAN_SURVEILLANCE.search(val):
            rx = (URBAN_EXPLICIT if URBAN_EXPLICIT.search(val)
                  else URBAN_INFRASTRUCTURE if URBAN_INFRASTRUCTURE.search(val) else PROJECT_URBAN_SURVEILLANCE)
            evidence.append(_ev("BioProject", key, val, "project_urban", rx))
            proj_urban = True
        if SPACE.search(val):
            evidence.append(_ev("BioProject", key, val, "non_urban_site", SPACE))
            proj_space = True
        if PROJECT_NONURBAN.search(val):
            evidence.append(_ev("BioProject", key, val, "project_non_urban", PROJECT_NONURBAN))
            proj_nonurban = True
        for cat, rx in CONTEXT_RX.items():
            if rx.search(val):
                contexts[cat] = contexts.get(cat, 0) + 1

    # --- Decision ---
    environmental = bool(env_src)
    if lab:
        status, reason = "non_environmental", "laboratory or culture material"
    elif nonenv_src and (not environmental or len(nonenv_src) > len(env_src)):
        status, reason = "non_environmental", "host-derived or clinical material"
    elif nonenv_src:
        status, reason = "uncertain", "conflicting evidence: environmental and host/clinical"
    elif not environmental:
        status, reason = "uncertain", "no environmental-sample evidence"
    elif proj_space or (site_neg and not (explicit or infra)):
        status, reason = "non_urban", "environmental sample from a non-urban site"
    elif explicit:
        status, reason = "confirmed_urban", "explicit urban setting at sample level"
    elif infra:
        status, reason = "confirmed_urban", "named urban infrastructure at sample level"
    elif proj_nonurban:
        status, reason = "non_urban", "BioProject indicates an agricultural or industrial source"
    elif plausible_city:
        status, reason = "probable_urban", "city-level location without sample-site confirmation"
    elif proj_urban:
        status, reason = "probable_urban", "urban context stated at BioProject level only"
    else:
        status, reason = "uncertain", "environmental sample; urban association not established"

    resolution, validation = _location(geo)
    ordered = [k for k, _ in sorted(contexts.items(), key=lambda kv: -kv[1])]
    return {
        "status": status,
        "reasons": [reason],
        "environmental_sample": status not in ("non_environmental",) and environmental,
        "urban_context": ordered[:3] if status in ("confirmed_urban", "probable_urban") else [],
        "location_resolution": resolution,
        "location_validation": validation,
        "evidence": evidence[:20],
        "decision_method": "rules",
        "rules_version": RULES_VERSION,
    }
