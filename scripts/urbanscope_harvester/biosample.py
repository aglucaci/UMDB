from __future__ import annotations
import re
import xml.etree.ElementTree as ET
from typing import Any, Dict, List

from .ncbi import http_get, _eutils_params
from .config import EUTILS
from .utils import _norm

COUNTRY_HINTS = {
    "usa": "United States",
    "u.s.a": "United States",
    "united states": "United States",
    "uk": "United Kingdom",
    "u.k.": "United Kingdom",
    "england": "United Kingdom",
    "scotland": "United Kingdom",
    "uae": "United Arab Emirates",
}

def efetch_biosample_xml(accession_or_uid: str):
    params = _eutils_params({"db": "biosample", "id": accession_or_uid, "retmode": "xml"})
    url = EUTILS + "efetch.fcgi?" + __import__("urllib.parse").parse.urlencode(params)
    xmltxt = http_get(url).decode(errors="replace")
    return xmltxt, url

def parse_biosample_attributes_from_xml(xmltxt: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {"attributes": {}}
    root = ET.fromstring(xmltxt.encode("utf-8", errors="ignore"))
    for attr in root.findall(".//Attribute"):
        key = (attr.attrib.get("attribute_name") or attr.attrib.get("harmonized_name") or "").strip()
        val = (attr.text or "").strip()
        if key and val:
            out["attributes"][key] = val
    title = root.findtext(".//Title") or ""
    organism = root.findtext(".//Organism/OrganismName") or root.findtext(".//OrganismName") or ""
    if title.strip():
        out["title"] = title.strip()
    if organism.strip():
        out["organism"] = organism.strip()
    return out

def get_biosample_details(biosample_accession: str, biosample_cache: Dict[str, Any]) -> Dict[str, Any]:
    biosample_accession = (biosample_accession or "").strip()
    if not biosample_accession:
        return {}
    if biosample_accession in biosample_cache:
        return biosample_cache[biosample_accession] or {}
    try:
        xmltxt, url = efetch_biosample_xml(biosample_accession)
        parsed = parse_biosample_attributes_from_xml(xmltxt)
        parsed["accession"] = biosample_accession
        parsed["efetch_url"] = url
        biosample_cache[biosample_accession] = parsed
        return parsed
    except Exception as e:
        biosample_cache[biosample_accession] = {"accession": biosample_accession, "error": str(e)}
        return biosample_cache[biosample_accession]

def biosample_display_card(bsd: Dict[str, Any]) -> Dict[str, Any]:
    """
    Make a small, UI-friendly subset for HTML display.
    Keeps the raw attributes, but also surfaces a few common ones.
    """
    if not isinstance(bsd, dict) or not bsd:
        return {}

    attrs = bsd.get("attributes", {}) if isinstance(bsd.get("attributes", {}), dict) else {}

    def pick(*keys):
        for k in keys:
            if k in attrs and str(attrs[k]).strip():
                return str(attrs[k]).strip()
        return ""

    # Common keys seen across BioSample records
    env_biome = pick("env_biome", "environment (biome)", "environment_biome")
    env_feature = pick("env_feature", "environment (feature)", "environment_feature")
    env_material = pick("env_material", "environment (material)", "environment_material")
    host = pick("host", "host scientific name", "host_taxid", "host_common_name")
    samp_type = pick("sample_type", "sample type", "isolation_source", "isolation source")
    collection_date = pick("collection_date", "collection date")
    sample_name = pick("sample_name", "sample name", "sample_name_alias")
    depth = pick("depth", "elevation", "altitude")
    temp = pick("temperature", "temp")
    ph = pick("ph")

    return {
        "accession": bsd.get("accession", ""),
        "title": bsd.get("title", ""),
        "organism": bsd.get("organism", ""),
        "collection_date": collection_date,
        "sample_name": sample_name,
        "sample_type": samp_type,
        "host": host,
        "env_biome": env_biome,
        "env_feature": env_feature,
        "env_material": env_material,
        "depth_or_altitude": depth,
        "temperature": temp,
        "ph": ph,
        "attributes": attrs,         # keep full attrs for deep UI drill-down
        "efetch_url": bsd.get("efetch_url", ""),
        "error": bsd.get("error", ""),
    }


US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
    "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "DC": "District of Columbia",
}
US_STATE_NAMES = {v.lower() for v in US_STATES.values()}

# Admin units below country level that are never a city.
ADMIN_SUFFIX_RE = re.compile(
    r"\b(province|state|county|prefecture|region|district|oblast|territory|governorate|"
    r"canton|department|municipality of|autonomous region)\b",
    re.I,
)
CANADIAN_PROVINCES = {
    "alberta", "british columbia", "manitoba", "new brunswick", "newfoundland and labrador",
    "nova scotia", "ontario", "prince edward island", "quebec", "saskatchewan",
}
CHINESE_PROVINCES = {
    "anhui", "fujian", "gansu", "guangdong", "guangxi", "guizhou", "hainan", "hebei",
    "heilongjiang", "henan", "hubei", "hunan", "jiangsu", "jiangxi", "jilin", "liaoning",
    "inner mongolia", "ningxia", "qinghai", "shaanxi", "shandong", "shanxi", "sichuan",
    "xinjiang", "tibet", "yunnan", "zhejiang",
}
CONTINENTS = {"africa", "asia", "europe", "north america", "south america", "oceania", "antarctica"}
PLACEHOLDERS = {
    "", "city", "cities", "multiple cities", "multiple", "various", "unknown", "missing",
    "not applicable", "not collected", "not provided", "na", "n/a", "none", "null",
    "restricted access", "not available",
}
MISSING_COUNTRY = {"missing", "not collected", "not provided", "not applicable", "unknown", "na", "n/a", "none"}


def _classify_geo_token(tok: str) -> str:
    """Return one of: placeholder, continent, us_state_abbrev, admin, locality."""
    low = tok.strip().lower().rstrip(".")
    if low in PLACEHOLDERS:
        return "placeholder"
    if low in CONTINENTS:
        return "continent"
    if tok.strip().upper() in US_STATES and len(tok.strip()) == 2:
        return "us_state_abbrev"
    if low in US_STATE_NAMES or low in CANADIAN_PROVINCES or low in CHINESE_PROVINCES or ADMIN_SUFFIX_RE.search(tok):
        return "admin"
    return "locality"


def parse_geo_loc_name(raw_geo: str) -> Dict[str, Any]:
    """Parse an INSDC geo_loc_name ("Country: free text") without guessing.

    The text after the colon is free-form and submitters order it either way
    ("NYC, NY" or "New York, Manhattan"), so tokens are classified rather than
    picked by position. A city is only reported when a token looks like a
    locality; states, provinces, counties and placeholders never become cities.
    Results are text-level only and are NOT validated against a gazetteer.
    """
    out = {"country": "", "region": "", "city": "", "locality_tokens": [], "city_status": "missing", "flags": []}
    raw_geo = (raw_geo or "").strip()
    if not raw_geo:
        return out

    if ":" in raw_geo:
        country, rest = raw_geo.split(":", 1)
    else:
        bits = [b.strip() for b in raw_geo.split(",") if b.strip()]
        country, rest = (bits[0], ", ".join(bits[1:])) if bits else ("", "")
        out["flags"].append("no_colon_delimiter")

    country = country.strip()
    if _norm(country) in MISSING_COUNTRY:
        out["flags"].append("country_placeholder")
        country = ""
    out["country"] = country

    tokens = [t.strip() for t in re.split(r"[,;]", rest) if t.strip()]
    kinds = [_classify_geo_token(t) for t in tokens]

    abbrev_states = {US_STATES[t.strip().upper()].lower() for t, k in zip(tokens, kinds) if k == "us_state_abbrev"}
    regions: List[str] = []
    localities: List[str] = []
    for tok, kind in zip(tokens, kinds):
        if kind == "us_state_abbrev":
            regions.append(US_STATES[tok.strip().upper()])
        elif kind == "admin":
            # "New York, NY" / "Washington, DC": a state name next to its own
            # abbreviation is the city of the same name.
            if tok.lower() in abbrev_states and not localities:
                localities.append(tok)
                out["flags"].append("city_same_name_as_state")
            else:
                regions.append(tok)
        elif kind == "locality":
            localities.append(tok)
        else:
            out["flags"].append(f"{kind}_token")

    out["region"] = regions[0] if regions else ""
    out["locality_tokens"] = localities
    if len(localities) == 1:
        out["city"] = localities[0]
        out["city_status"] = "parsed"
    elif len(localities) > 1:
        # Ambiguous order; keep the first locality (INSDC convention is
        # general -> specific, but the most common submitter pattern is
        # "City, State" which has already removed the state token).
        out["city"] = localities[0]
        out["city_status"] = "ambiguous"
        out["flags"].append("multiple_locality_tokens")
    elif regions:
        out["city_status"] = "region_only"
    elif tokens:
        out["city_status"] = "placeholder"
    return out


def infer_geo(biosample_details: Dict[str, Any], fallbacks: List[str]) -> Dict[str, Any]:
    attrs = (biosample_details or {}).get("attributes", {}) if isinstance(biosample_details, dict) else {}
    raw_geo = ""
    raw_geo_field = ""

    for k in ["geo_loc_name", "geographic location", "geographic_location", "geographic location (country and/or sea)", "country", "location"]:
        if k in attrs and attrs[k]:
            raw_geo = str(attrs[k]).strip()
            raw_geo_field = k
            break

    lat = lon = ""
    latlon = ""
    for k in ["lat_lon", "latitude and longitude", "latitude_longitude"]:
        if k in attrs and attrs[k]:
            latlon = str(attrs[k]).strip()
            break
    if latlon:
        m = re.search(r"(-?\d+(?:\.\d+)?)\s*([NS])?\s*[, ]\s*(-?\d+(?:\.\d+)?)\s*([EW])?", latlon, re.I)
        if m:
            lat_v, ns, lon_v, ew = m.group(1), (m.group(2) or "").upper(), m.group(3), (m.group(4) or "").upper()
            if ns == "S" and not lat_v.startswith("-"):
                lat_v = "-" + lat_v
            if ew == "W" and not lon_v.startswith("-"):
                lon_v = "-" + lon_v
            try:
                if -90 <= float(lat_v) <= 90 and -180 <= float(lon_v) <= 180:
                    lat, lon = lat_v, lon_v
            except ValueError:
                pass

    parsed = parse_geo_loc_name(raw_geo)
    country = parsed["country"]
    country_source = "biosample" if country else ""

    c_norm = _norm(country)
    if c_norm in COUNTRY_HINTS:
        country = COUNTRY_HINTS[c_norm]
    elif country:
        country = " ".join(w.capitalize() for w in country.split())

    if not country and fallbacks:
        blob = _norm(" | ".join([x for x in fallbacks if x]))
        for k, v in COUNTRY_HINTS.items():
            if re.search(rf"\b{re.escape(k)}\b", blob):
                country = v
                country_source = "text_fallback"
                break

    return {
        "country": country,
        "region": parsed["region"],
        "city": parsed["city"],
        "lat": lat,
        "lon": lon,
        "raw": raw_geo,
        "raw_field": raw_geo_field,
        "country_source": country_source,
        "city_status": parsed["city_status"],
        "locality_tokens": parsed["locality_tokens"],
        "parse_flags": parsed["flags"],
        "resolution": "text_parsed_unvalidated",
        "collection_date": str(attrs.get("collection_date") or attrs.get("collection date") or "").strip(),
        "biosample_accession": (biosample_details or {}).get("accession", ""),
    }
