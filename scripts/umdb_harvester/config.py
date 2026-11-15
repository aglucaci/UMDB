from __future__ import annotations
import os, re


def _load_dotenv(path: str = ".env") -> None:
    """Load KEY=value lines from a local .env file (stdlib only).

    Variables already set in the environment win, so CI secrets and one-off
    `export`s override the file. Blank lines and # comments are ignored;
    surrounding single/double quotes are stripped.
    """
    try:
        with open(path, encoding="utf-8") as fh:
            lines = fh.readlines()
    except OSError:
        return
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.removeprefix("export ").strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        if key and key not in os.environ:
            os.environ[key] = value


_load_dotenv(os.getenv("UMDB_ENV_FILE", ".env"))

# NCBI / Eutils
EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
NCBI_API_KEY = os.getenv("NCBI_API_KEY", "").strip()
TOOL_NAME = os.getenv("NCBI_TOOL", "umdb-srr-harvester")
NCBI_EMAIL = os.getenv("NCBI_EMAIL", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()

# Bump whenever any profile string changes; recorded in harvest reports so a
# release can state exactly which discovery queries produced it.
#   2026.11   stop excluding soil/sediment/freshwater/river/lake and "plant".
#   2026.11.2 replace the five broad profiles (kept below as
#             LEGACY_QUERY_PROFILES) with seven urban-environment profiles built
#             on NCBI environment organism labels ([ORGN]) and urban phrases.
#             No catch-all terms ("surface", "air", "swab", "station" alone), no
#             global NOT block. Word forms are spelled out because NCBI drops
#             "ITS" as a stopword and truncates metagenom* to 600 variants.
#             SARS-CoV-2 targeted amplicon surveillance moved to an opt-in
#             profile. On 2025 releases: ~64k experiments instead of ~600k+, with
#             98% of the 2024 confirmed-urban and 95% of probable-urban catalog
#             runs still found and 99.5% of non-environmental runs no longer matched.
QUERY_PROFILES_VERSION = "2026.11.2"

# Method words: any microbial-community assay.
_METHOD = (
    '(microbiome OR microbiomes OR microbiota OR metagenome OR metagenomes OR metagenomic OR metagenomics '
    'OR metatranscriptome OR metatranscriptomic OR metatranscriptomics OR virome OR amplicon OR "16S" '
    'OR "16S rRNA" OR "18S" OR "internal transcribed spacer" OR ITS1 OR ITS2)'
)
# Targeted SARS-CoV-2 amplicon panels (public-health wastewater surveillance).
_SARS_COV_2_AMPLICON = '(("SARS-CoV-2" OR "severe acute respiratory syndrome coronavirus 2"[ORGN]) AND amplicon[Strategy])'
_WASTEWATER = (
    '("wastewater metagenome"[ORGN] OR "activated sludge metagenome"[ORGN] OR "sludge metagenome"[ORGN] '
    'OR ((wastewater OR sewage OR sewer OR influent) AND (municipal OR urban OR city OR "treatment plant" '
    'OR WWTP OR sewershed OR community)))'
)

QUERY_PROFILES = {
    # Municipal wastewater and sewage microbiomes (shotgun, metatranscriptomic, amplicon).
    "municipal_wastewater": f'({_WASTEWATER} NOT {_SARS_COV_2_AMPLICON})',
    # Urban and indoor air, aerosols, dust.
    "air_dust": (
        '("air metagenome"[ORGN] OR "aerosol metagenome"[ORGN] OR "dust metagenome"[ORGN] OR '
        '(("urban air" OR "city air" OR bioaerosol OR bioaerosols OR "road dust" OR "particulate matter") AND '
        + _METHOD + '))'
    ),
    # Buildings, classrooms, offices, indoor surfaces.
    "built_environment": (
        '("indoor metagenome"[ORGN] OR (("built environment" OR "built-environment" OR "indoor microbiome" '
        'OR "indoor air" OR "public building" OR classroom OR classrooms OR "office building") AND ' + _METHOD + '))'
    ),
    # Samples labelled with NCBI's urban metagenome taxon.
    "urban_metagenome": '"urban metagenome"[ORGN]',
    # Subways, stations, public transport surfaces.
    "transit": (
        '((subway OR "metro station" OR "public transit" OR "public transport" OR "public transportation" '
        'OR "railway station" OR "train station" OR "bus station" OR turnstile OR turnstiles OR handrail '
        'OR handrails) AND ' + _METHOD + ')'
    ),
    # Urban soils, parks, greenspace, roadsides.
    "urban_soil_greenspace": (
        '(("urban soil" OR "urban soils" OR "urban park" OR "city park" OR "urban green" OR greenspace '
        'OR "green space" OR roadside OR "urban garden") AND ' + _METHOD + ')'
    ),
    # Stormwater, urban rivers and canals, combined sewers.
    "urban_water_drainage": (
        '((stormwater OR "storm water" OR "storm drain" OR "urban river" OR "urban stream" OR "urban lake" '
        'OR "urban canal" OR "combined sewer" OR "urban runoff") AND ' + _METHOD + ')'
    ),
    # Opt-in (not in the defaults): targeted SARS-CoV-2 amplicon wastewater surveillance.
    # Roughly 58k experiments in 2025 alone; pathogen panels, not microbiome studies.
    "wastewater_sars_cov_2": f'({_WASTEWATER} AND {_SARS_COV_2_AMPLICON})',
}

DEFAULT_QUERY_PROFILE_NAMES = [
    "municipal_wastewater",
    "air_dust",
    "built_environment",
    "urban_metagenome",
    "transit",
    "urban_soil_greenspace",
    "urban_water_drainage",
]

# Profiles used for releases up to April 2026 (QUERY_PROFILES_VERSION 2026.11
# and earlier). Kept so those harvests can be reproduced: pass them with
# --query "$(python3 -c '...LEGACY_QUERY_PROFILES[...]')" if ever needed.
LEGACY_QUERY_PROFILES = {
    "urban_shotgun": (
        '('
          '('
            '"urban" OR "city" OR "cities" OR metropolitan OR municipal OR '
            '"built environment" OR "built-environment" OR '
            'subway OR metro OR transit OR railway OR airport OR station OR '
            '"public transit" OR "public transport" OR '
            'wastewater OR sewage OR sewer OR stormwater OR '
            'street OR sidewalk OR pavement OR road OR '
            'building OR buildings OR housing OR dormitory OR hospital OR school OR campus OR '
            '"surface swab" OR fomite OR surface OR '
            'air OR aerosol OR dust'
          ') '
          'AND '
          '('
            '"whole genome shotgun" OR '
            '"shotgun metagenom*" OR '
            '"shotgun sequencing" OR '
            'metagenom* OR '
            'metatranscriptom* OR '
            '"total RNA sequencing" OR '
            '"metatranscriptome sequencing" OR '
            'microbiome OR microbiota'
          ') '
          'AND '
          '('
            'environment* OR "built environment" OR wastewater OR sewage OR sewer OR stormwater OR '
            'surface OR swab OR air OR aerosol OR dust'
          ') '
          'NOT '
          '('
            '"single-cell" OR scRNA OR "whole exome" OR WES OR '
            'marine OR ocean OR '
            'forest OR agricultur* OR farm OR rhizosphere'
          ')'
        ')'
    ),
    "urban_amplicon": (
        '('
          '('
            '"urban" OR "city" OR "cities" OR metropolitan OR municipal OR '
            '"built environment" OR "built-environment" OR '
            'subway OR metro OR transit OR railway OR airport OR station OR '
            'wastewater OR sewage OR sewer OR stormwater OR '
            'street OR sidewalk OR pavement OR road OR '
            'building OR buildings OR housing OR hospital OR school OR campus OR '
            'surface OR swab OR fomite OR air OR aerosol OR dust'
          ') '
          'AND '
          '('
            'amplicon OR "marker gene" OR "16S" OR "16S rRNA" OR "18S" OR ITS OR '
            '"V3-V4" OR "V4 region" OR "internal transcribed spacer" OR microbiome OR microbiota'
          ') '
          'AND '
          '('
            'environment* OR wastewater OR sewage OR sewer OR stormwater OR '
            '"built environment" OR surface OR swab OR air OR aerosol OR dust'
          ') '
          'NOT '
          '('
            '"single-cell" OR scRNA OR "whole exome" OR WES OR '
            'marine OR ocean OR '
            'forest OR agricultur* OR farm OR rhizosphere'
          ')'
        ')'
    ),
    "wastewater_surveillance": (
        '('
          '('
            'wastewater OR sewage OR sewer OR stormwater OR influent OR effluent OR sludge OR '
            '"waste water" OR "waste-water"'
          ') '
          'AND '
          '('
            'urban OR city OR cities OR metropolitan OR municipal OR community OR public'
          ') '
          'AND '
          '('
            'microbiome OR microbiota OR metagenom* OR metatranscriptom* OR '
            'amplicon OR "16S" OR ITS OR virome'
          ')'
        ')'
    ),
    "transit_and_surface": (
        '('
          '('
            'subway OR metro OR transit OR railway OR train OR bus OR airport OR terminal OR station OR '
            '"public transit" OR "public transport" OR '
            'surface OR swab OR fomite OR handrail OR turnstile OR bench OR kiosk'
          ') '
          'AND '
          '('
            'microbiome OR microbiota OR metagenom* OR metatranscriptom* OR amplicon OR "16S" OR ITS'
          ')'
        ')'
    ),
    "air_and_dust": (
        '('
          '('
            'urban OR city OR cities OR metropolitan OR municipal OR '
            '"built environment" OR building OR hospital OR school OR classroom OR office'
          ') '
          'AND '
          '('
            'air OR aerosol OR bioaerosol OR dust OR particulate'
          ') '
          'AND '
          '('
            'microbiome OR microbiota OR metagenom* OR metatranscriptom* OR amplicon OR "16S" OR ITS'
          ')'
        ')'
    )
}


DEFAULT_QUERY = QUERY_PROFILES["municipal_wastewater"]








# Paths
DATA_DIR = "data"
DOCS_DIR = "docs"
DB_DIR = f"{DOCS_DIR}/db"
CACHE_DIR = f"{DATA_DIR}/cache"
DEBUG_DIR = f"{DATA_DIR}/debug"
DOCS_DEBUG_DIR = f"{DOCS_DIR}/debug"

SEEN_SRA_UIDS = f"{DATA_DIR}/seen_sra_uids.txt"
SEEN_SRR_RUNS = f"{DATA_DIR}/seen_srr_runs.txt"

BIOSAMPLE_CACHE = f"{CACHE_DIR}/biosample.json"
BIOPROJECT_CACHE = f"{CACHE_DIR}/bioproject.json"
BIOPROJECT_UID_CACHE = f"{CACHE_DIR}/bioproject_uid.json"
AI_CURATION_CACHE = f"{CACHE_DIR}/ai_curation.json"

DOCS_LATEST_SRR = f"{DOCS_DIR}/latest_srr.json"
DOCS_LATEST_DEBUG = f"{DOCS_DEBUG_DIR}/latest_report.json"

BIOPROJECT_RE = re.compile(r"\bPRJ(?:NA|EB|DB)\d+\b", re.I)

# 50MB limit
MAX_OUTPUT_BYTES = 50 * 1024 * 1024
