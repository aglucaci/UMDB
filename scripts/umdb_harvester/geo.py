"""Gazetteer validation of sample locations (GeoNames + ISO 3166).

`biosample.parse_geo_loc_name` splits the free-text INSDC `geo_loc_name`
into country / region / locality tokens. This module turns that text into
*accepted* geographic annotations tied to reference identifiers:

  country   ISO 3166-1 alpha-2 (GeoNames countryInfo)
  region    GeoNames admin1 code, e.g. "US.NY"
  place     GeoNames populated-place id (cities500: places with >= 500 people)

Every sample gets exactly one annotation per release, with a validation
status, the method that produced it and the evidence (matched text, distance
to the reported coordinates). The rule throughout: a missing city is better
than a wrong one. Country-only records are kept; a place is assigned only
when the text names it unambiguously (or the coordinates confirm it).

Place status (`status`):
  verified     the text names exactly one place in the country (after any
               region constraint), and reported coordinates, if any, lie
               within verify_radius_km() of it (30 km, more for large cities;
               coordinates given to whole degrees are too coarse to count)
  probable     the place was chosen among homonyms by population dominance,
               or inferred from coordinates alone (the most populous place
               whose extent, scaled by population, contains the point; never
               an administrative unit such as a county), or the region
               constraint had to be dropped
  ambiguous    several places match and nothing decides between them
  conflicting  the text names one place unambiguously but precise
               coordinates are far from it
  unresolved   no place could be identified (the country may still be known)

Country status (`country_status`):
  verified     from the BioSample location field, consistent with coordinates
  probable     from a loose alias, a text fallback, or from coordinates alone
  conflicting  coordinates lie in a different country (no place of the stated
               country within BORDER_KM)
  unresolved   no country (oceans, placeholders, codes)

GeoNames data are CC BY 4.0: attribution is required wherever the derived
annotations are published (see the website's Data and Methodology pages).
"""
from __future__ import annotations

import io
import json
import math
import os
import re
import time
import unicodedata
import zipfile
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .config import DATA_DIR
from .utils import log, warn, utc_now

GEO_RULES_VERSION = "2026.11.2"
GEONAMES_DIR = os.path.join(DATA_DIR, "reference", "geonames")
GEONAMES_BASE = "https://download.geonames.org/export/dump/"
GEONAMES_FILES = ("countryInfo.txt", "admin1CodesASCII.txt", "cities500.zip")
SOURCE_FILE = os.path.join(GEONAMES_DIR, "SOURCE.json")

VERIFY_KM = 30.0      # coordinates this close to the named place confirm it (grows with population)
COORD_KM = 25.0       # coordinates alone: search radius; a place must also *contain* the point (place_extent_km)
BORDER_KM = 50.0      # a stated country is contradicted only if none of its places is this close
CLUSTER_KM = 30.0     # several matched tokens this close together name one place (city + district)
DOMINANCE = 5.0       # a homonym wins on population only if it is this many times larger

# Feature codes that are settlements in their own right. Excluded: PPLX
# (sections of a city), PPLH/PPLQ/PPLW (historical, abandoned, destroyed),
# PPLCH (historical capital), PPLF (farm village).
PLACE_CODES = {"PPL", "PPLA", "PPLA2", "PPLA3", "PPLA4", "PPLA5", "PPLC", "PPLG", "PPLL", "PPLR", "PPLS", "STLMT"}

STATUSES = ("verified", "probable", "ambiguous", "conflicting", "unresolved")

# Country spellings found in BioSample metadata that GeoNames' names miss.
# The second element marks loose aliases, which only reach "probable".
COUNTRY_ALIASES: Dict[str, Tuple[str, bool]] = {
    "usa": ("US", False), "united states of america": ("US", False), "us": ("US", False),
    "uk": ("GB", False), "great britain": ("GB", False), "england": ("GB", False), "scotland": ("GB", False),
    "wales": ("GB", False), "northern ireland": ("GB", False),
    "netherlands": ("NL", False), "the netherlands": ("NL", False), "holland": ("NL", True),
    "czech republic": ("CZ", False), "czechia": ("CZ", False),
    "viet nam": ("VN", False), "cote d ivoire": ("CI", False), "ivory coast": ("CI", False),
    "west bank": ("PS", False), "gaza strip": ("PS", False), "palestine": ("PS", False),
    "macedonia": ("MK", False), "north macedonia": ("MK", False),
    "macau": ("MO", False), "macao": ("MO", False), "hong kong": ("HK", False),
    "svalbard": ("SJ", False), "south korea": ("KR", False), "republic of korea": ("KR", False),
    "korea": ("KR", True), "north korea": ("KP", False), "russia": ("RU", False),
    "russian federation": ("RU", False), "iran": ("IR", False), "syria": ("SY", False),
    "laos": ("LA", False), "bolivia": ("BO", False), "venezuela": ("VE", False), "tanzania": ("TZ", False),
    "moldova": ("MD", False), "brunei": ("BN", False), "taiwan": ("TW", False), "turkey": ("TR", False),
    "turkiye": ("TR", False), "democratic republic of the congo": ("CD", False),
    "republic of the congo": ("CG", False), "eswatini": ("SZ", False), "swaziland": ("SZ", False),
    "cape verde": ("CV", False), "micronesia": ("FM", False), "east timor": ("TL", False),
    "puerto rico": ("PR", False), "sanjuan puerto rico": ("PR", True), "mayagez": ("PR", True),
    "south west germany": ("DE", True), "buenos aires": ("AR", True), "nanjing": ("CN", True),
    "san francisco bay area": ("US", True),
}
# Territories that ISO 3166 lists separately but submitters file under a parent.
TERRITORY_TOKENS: Dict[str, Dict[str, str]] = {
    "US": {"puerto rico": "PR", "guam": "GU", "us virgin islands": "VI", "virgin islands": "VI",
           "american samoa": "AS", "northern mariana islands": "MP"},
    "CN": {"hong kong": "HK", "macau": "MO", "macao": "MO", "taiwan": "TW"},
    "GB": {"gibraltar": "GI", "bermuda": "BM", "jersey": "JE", "guernsey": "GG", "isle of man": "IM"},
    "FR": {"french guiana": "GF", "reunion": "RE", "martinique": "MQ", "guadeloupe": "GP",
           "new caledonia": "NC", "french polynesia": "PF", "mayotte": "YT"},
    "DK": {"greenland": "GL", "faroe islands": "FO"},
    "NO": {"svalbard": "SJ"},
    "NL": {"aruba": "AW", "curacao": "CW"},
}
# Territories where the country and its city coincide: a country-only record
# is placed in the city (probable, method "city-state").
CITY_STATES = {"HK", "SG", "MO", "MC", "VA", "GI"}
_TOKEN_SUFFIXES = re.compile(r"\s+(city|shi|si|ward|ku|district|municipality|town|village|county)$")
_PAREN = re.compile(r"\(([^)]*)\)")


def norm(s: Any) -> str:
    """Lowercase ASCII with punctuation collapsed: 'São Paulo' -> 'sao paulo'."""
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 6371.0 * 2 * math.asin(min(1.0, math.sqrt(a)))


def coord_decimals(x: float) -> int:
    """Decimal places a coordinate was reported with (39.58 -> 2, -15.0 -> 0)."""
    s = repr(float(x))
    if "e" in s or "." not in s:
        return 0
    frac = s.split(".", 1)[1].rstrip("0")
    return len(frac)


# Names that are administrative units, not settlements ("Norfolk County",
# "Wujiang District"): never inferred as a city from coordinates alone.
ADMIN_NAME_RE = re.compile(r"\b(county|district|municipality|township|region|province|prefecture|parish|borough|"
                           r"shire|canton|department|governorate|oblast|regional)\b", re.I)


def place_extent_km(population: int) -> float:
    """Rough radius of a settlement: ~2.5 km for a village, ~10 km at 50,000
    people, at most 25 km for a large city. Coordinates outside it are not
    taken to lie in that place."""
    return min(COORD_KM, 1.5 + 4.0 * math.sqrt(max(population, 0) / 1e4))


def verify_radius_km(population: int, decimals: int) -> float:
    """How far precise coordinates may lie from a named place and still confirm it.

    Large cities (and Chinese prefecture-level cities) span tens of km, so the
    radius grows with population; one-decimal coordinates add their ~11 km error.
    """
    r = VERIFY_KM + 15.0 * math.sqrt(max(population, 0) / 1e6)
    return r + (11.0 if decimals == 1 else 0.0)


# --------------------------------------------------------------------------
# Reference data
# --------------------------------------------------------------------------

def fetch_geonames(force: bool = False, directory: str = GEONAMES_DIR) -> Dict[str, Any]:
    """Download the GeoNames reference files (about 14 MB) once."""
    import hashlib
    from .ncbi import http_get

    os.makedirs(directory, exist_ok=True)
    source = {"source": GEONAMES_BASE, "license": "CC BY 4.0 (https://www.geonames.org/)",
              "downloaded_utc": utc_now(), "files": {}}
    for name in GEONAMES_FILES:
        path = os.path.join(directory, name)
        fresh = force or not os.path.exists(path)
        if fresh:
            log(f"Downloading {GEONAMES_BASE}{name}")
            data = http_get(GEONAMES_BASE + name)
            with open(path + ".part", "wb") as fh:
                fh.write(data)
            os.replace(path + ".part", path)
        with open(path, "rb") as fh:
            source["files"][name] = {"bytes": os.path.getsize(path), "sha256": hashlib.sha256(fh.read()).hexdigest(),
                                     "downloaded_utc": utc_now() if fresh else
                                     time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(os.path.getmtime(path)))}
    source["downloaded_utc"] = min(f["downloaded_utc"] for f in source["files"].values())
    with open(os.path.join(directory, "SOURCE.json"), "w") as fh:
        json.dump(source, fh, indent=2)
    return source


def gazetteer_available(directory: str = GEONAMES_DIR) -> bool:
    return all(os.path.exists(os.path.join(directory, f)) for f in GEONAMES_FILES)


class Gazetteer:
    """In-memory GeoNames index: countries, admin1 regions, populated places."""

    def __init__(self, directory: str = GEONAMES_DIR):
        self.directory = directory
        self.countries: Dict[str, Dict[str, Any]] = {}
        self.country_by_name: Dict[str, Tuple[str, bool]] = {}
        self.admin1: Dict[str, Dict[str, Any]] = {}            # "US.NY" -> {name, country}
        self.admin1_by_name: Dict[Tuple[str, str], str] = {}   # (iso2, norm name) -> "US.NY"
        # Places as parallel lists (compact); index i is a place.
        self.p_id: List[int] = []
        self.p_name: List[str] = []
        self.p_country: List[str] = []
        self.p_admin1: List[str] = []
        self.p_lat: List[float] = []
        self.p_lon: List[float] = []
        self.p_pop: List[int] = []
        self.p_code: List[str] = []
        self.by_name: Dict[Tuple[str, str], List[int]] = defaultdict(list)
        self.by_alt: Dict[Tuple[str, str], List[int]] = defaultdict(list)
        self.grid: Dict[Tuple[int, int], List[int]] = defaultdict(list)
        self.source: Dict[str, Any] = {}
        self._load()

    # -- loading -----------------------------------------------------------
    def _load(self) -> None:
        d = self.directory
        with open(os.path.join(d, "countryInfo.txt"), encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("#") or not line.strip():
                    continue
                f = line.rstrip("\n").split("\t")
                iso2, iso3, name = f[0], f[1], f[4]
                self.countries[iso2] = {"iso2": iso2, "iso3": iso3, "numeric": f[2], "name": name, "continent": f[8]}
                for key in (name, iso3, iso2):
                    self.country_by_name.setdefault(norm(key), (iso2, False))
        for alias, value in COUNTRY_ALIASES.items():
            self.country_by_name[norm(alias)] = value

        with open(os.path.join(d, "admin1CodesASCII.txt"), encoding="utf-8") as fh:
            for line in fh:
                f = line.rstrip("\n").split("\t")
                if len(f) < 4:
                    continue
                code, name, ascii_name = f[0], f[1], f[2]
                iso2 = code.split(".")[0]
                self.admin1[code] = {"code": code, "name": name, "country": iso2, "geonames_id": int(f[3] or 0)}
                for key in {norm(name), norm(ascii_name), _strip_admin_word(norm(ascii_name))}:
                    if key:
                        self.admin1_by_name.setdefault((iso2, key), code)
        # US state abbreviations ("TX") as admin1 names.
        from .biosample import US_STATES
        for abbr, name in US_STATES.items():
            code = self.admin1_by_name.get(("US", norm(name)))
            if code:
                self.admin1_by_name[("US", norm(abbr))] = code

        with zipfile.ZipFile(os.path.join(d, "cities500.zip")) as zf:
            member = next(n for n in zf.namelist() if n.endswith(".txt"))
            with zf.open(member) as raw:
                for line in io.TextIOWrapper(raw, encoding="utf-8"):
                    f = line.rstrip("\n").split("\t")
                    if len(f) < 15 or f[7] not in PLACE_CODES:
                        continue
                    i = len(self.p_id)
                    iso2 = f[8]
                    lat, lon = float(f[4]), float(f[5])
                    self.p_id.append(int(f[0]))
                    self.p_name.append(f[1])
                    self.p_country.append(iso2)
                    self.p_admin1.append(f"{iso2}.{f[10]}" if f[10] else "")
                    self.p_lat.append(lat)
                    self.p_lon.append(lon)
                    self.p_pop.append(int(f[14] or 0))
                    self.p_code.append(f[7])
                    primary = {norm(f[1]), norm(f[2])} - {""}
                    for key in primary:
                        self.by_name[(iso2, key)].append(i)
                    for alt in f[3].split(","):
                        key = norm(alt)
                        if key and key not in primary and len(key) <= 40:
                            self.by_alt[(iso2, key)].append(i)
                    self.grid[(math.floor(lat), math.floor(lon))].append(i)
        if os.path.exists(os.path.join(d, "SOURCE.json")):
            with open(os.path.join(d, "SOURCE.json")) as fh:
                self.source = json.load(fh)

    # -- lookups -----------------------------------------------------------
    def country(self, text: str) -> Tuple[Optional[str], bool]:
        """(iso2, loose) for a country name, or (None, False)."""
        key = norm(text)
        if not key:
            return None, False
        return self.country_by_name.get(key, (None, False))

    def region(self, iso2: str, text: str) -> Optional[str]:
        key = norm(text)
        if not key:
            return None
        return self.admin1_by_name.get((iso2, key)) or self.admin1_by_name.get((iso2, _strip_admin_word(key)))

    def places_named(self, iso2: str, text: str) -> List[int]:
        """Place indexes named `text` in `iso2`: primary names first, then alternates."""
        key = norm(text)
        if not key:
            return []
        for variant in _variants(key):
            hits = self.by_name.get((iso2, variant)) or self.by_alt.get((iso2, variant))
            if hits:
                return list(hits)
        return []

    def nearest(self, lat: float, lon: float, max_km: float, iso2: Optional[str] = None) -> Tuple[Optional[int], float]:
        """Nearest place (optionally in one country) within max_km."""
        best, best_d = None, max_km
        reach = 1 + int(max_km / 111.0) + (1 if abs(lat) > 60 else 0)
        cy, cx = math.floor(lat), math.floor(lon)
        for dy in range(-reach, reach + 1):
            for dx in range(-reach, reach + 1):
                gx = ((cx + dx + 180) % 360) - 180
                for i in self.grid.get((cy + dy, gx), ()):
                    if iso2 and self.p_country[i] != iso2:
                        continue
                    d = haversine_km(lat, lon, self.p_lat[i], self.p_lon[i])
                    if d <= best_d:
                        best, best_d = i, d
        return best, best_d

    def largest_within(self, lat: float, lon: float, max_km: float, iso2: Optional[str] = None) -> Tuple[Optional[int], float]:
        """Most populous settlement (optionally in one country) whose extent
        contains the point: within max_km and within place_extent_km of it,
        excluding administrative-unit names."""
        best, best_pop, best_d = None, -1, 0.0
        reach = 1 + int(max_km / 111.0) + (1 if abs(lat) > 60 else 0)
        cy, cx = math.floor(lat), math.floor(lon)
        for dy in range(-reach, reach + 1):
            for dx in range(-reach, reach + 1):
                gx = ((cx + dx + 180) % 360) - 180
                for i in self.grid.get((cy + dy, gx), ()):
                    if iso2 and self.p_country[i] != iso2:
                        continue
                    if self.p_pop[i] <= best_pop or ADMIN_NAME_RE.search(self.p_name[i]):
                        continue
                    d = haversine_km(lat, lon, self.p_lat[i], self.p_lon[i])
                    if d <= max_km and d <= place_extent_km(self.p_pop[i]):
                        best, best_pop, best_d = i, self.p_pop[i], d
        return best, best_d

    def capital(self, iso2: str) -> Optional[int]:
        """The country's national capital (PPLC), else its most populous place."""
        if not hasattr(self, "_capitals"):
            caps: Dict[str, int] = {}
            for i, c in enumerate(self.p_country):
                b = caps.get(c)
                if b is None or (self.p_code[i] == "PPLC", self.p_pop[i]) > (self.p_code[b] == "PPLC", self.p_pop[b]):
                    caps[c] = i
            self._capitals = caps
        return self._capitals.get(iso2)

    def place_record(self, i: int) -> Dict[str, Any]:
        return {"geonames_id": self.p_id[i], "name": self.p_name[i], "country_iso2": self.p_country[i],
                "admin1_code": self.p_admin1[i] or None, "lat": self.p_lat[i], "lon": self.p_lon[i],
                "population": self.p_pop[i], "feature_code": self.p_code[i]}


def _strip_admin_word(key: str) -> str:
    return re.sub(r"\b(province|state|prefecture|region|oblast|county|governorate|canton|department|"
                  r"autonomous region|special administrative region|municipality|metropolitan city|"
                  r"of|the)\b", " ", key).strip().replace("  ", " ")


def _variants(key: str) -> List[str]:
    out = [key]
    stripped = _TOKEN_SUFFIXES.sub("", key)
    if stripped != key and stripped:
        out.append(stripped)
    if key.startswith("st "):
        out.append("saint " + key[3:])
    if key.startswith("saint "):
        out.append("st " + key[6:])
    return out


# --------------------------------------------------------------------------
# Resolution
# --------------------------------------------------------------------------

def _tokens(raw: str, country_text: str) -> List[str]:
    """Location tokens after the country, with parenthesised text as extra tokens."""
    rest = raw.split(":", 1)[1] if ":" in raw else ""
    if not rest and "," in raw:
        rest = raw.split(",", 1)[1]
    extra = _PAREN.findall(rest)
    rest = _PAREN.sub(" ", rest)
    toks = [t.strip(" .-_") for t in re.split(r"[,;:/|]", rest)]
    toks += [t.strip() for e in extra for t in re.split(r"[,;]", e)]
    out = []
    for t in toks:
        if not t or norm(t) == norm(country_text):
            continue
        out.append(t)
        # "Houston TX" / "Seattle WA" without a comma
        m = re.match(r"^(.*\S)\s+([A-Z]{2})$", t)
        if m:
            out.extend([m.group(1), m.group(2)])
    return out


class Resolver:
    """Resolves (location text, coordinates) to one accepted annotation.

    Text resolution is cached per distinct (raw text, country, source), so a
    catalog with hundreds of thousands of samples but a few thousand distinct
    location strings resolves in seconds.
    """

    def __init__(self, gaz: Gazetteer):
        self.g = gaz
        self._text_cache: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        self._xy_cache: Dict[tuple, Any] = {}

    def _cached(self, kind: str, fn, lat: float, lon: float, km: float, iso2: Optional[str]):
        key = (kind, round(lat, 4), round(lon, 4), km, iso2)
        hit = self._xy_cache.get(key)
        if hit is None:
            hit = self._xy_cache[key] = fn(lat, lon, km, iso2)
        return hit

    # -- text --------------------------------------------------------------
    def resolve_text(self, raw: str, country_text: str, country_source: str = "biosample") -> Dict[str, Any]:
        key = (raw or "", country_text or "", country_source or "")
        hit = self._text_cache.get(key)
        if hit is None:
            hit = self._text_cache[key] = self._resolve_text(*key)
        return hit

    def _resolve_text(self, raw: str, country_text: str, country_source: str) -> Dict[str, Any]:
        g = self.g
        out: Dict[str, Any] = {"country": None, "country_loose": False, "admin1": None,
                               "candidates": [], "match": "none", "matched_text": None, "region_dropped": False}
        iso2, loose = g.country(country_text)
        if not iso2 and raw:
            iso2, loose = g.country(raw.split(":", 1)[0])
        if not iso2:
            return out
        tokens = _tokens(raw, country_text) if raw else []
        # Territories filed under a parent country ("USA: Puerto Rico").
        for t in tokens:
            sub = TERRITORY_TOKENS.get(iso2, {}).get(norm(t))
            if sub:
                iso2 = sub
                tokens = [x for x in tokens if x is not t]
                break
        out["country"], out["country_loose"] = iso2, loose or country_source == "text_fallback"

        regions: List[Tuple[str, str]] = []
        place_tokens: List[str] = []
        for t in tokens:
            code = g.region(iso2, t)
            if code:
                regions.append((t, code))
            else:
                place_tokens.append(t)
        admin1 = regions[0][1] if regions else None
        out["admin1"] = admin1

        # Candidate places per token.
        groups: List[Tuple[str, List[int]]] = []
        for t in place_tokens:
            hits = g.places_named(iso2, t)
            if hits:
                groups.append((t, hits))
        if not groups:
            # "Tokyo" / "New York, NY": the region shares its name with its main city.
            # The place must be a capital or a sizeable city (New York City is a
            # plain PPL in GeoNames), so "Georgia" never becomes a village.
            for t, code in regions:
                hits = [i for i in g.places_named(iso2, t) if g.p_admin1[i] == code
                        and (g.p_code[i] in ("PPLC", "PPLA", "PPLA2") or g.p_pop[i] >= 50000)]
                if hits:
                    groups.append((t, [max(hits, key=lambda i: g.p_pop[i])]))
                    out["region_city"] = True
                    break
        if not groups:
            return out

        def constrain(hits: List[int]) -> Tuple[List[int], bool]:
            if not admin1:
                return hits, False
            inside = [i for i in hits if g.p_admin1[i] == admin1]
            return (inside, False) if inside else (hits, True)

        picks: List[Tuple[str, List[int], bool]] = []
        for t, hits in groups:
            kept, dropped = constrain(hits)
            picks.append((t, kept, dropped))

        # Several tokens matched (city + district, or two unrelated places).
        # Keep the candidate set of the token whose best match is most populous
        # when all tokens' best matches lie within CLUSTER_KM of it.
        if len(picks) > 1:
            best_of = [(t, max(c, key=lambda i: g.p_pop[i]), c, dr) for t, c, dr in picks]
            anchor = max(best_of, key=lambda x: g.p_pop[x[1]])
            near = all(haversine_km(g.p_lat[b], g.p_lon[b], g.p_lat[anchor[1]], g.p_lon[anchor[1]]) <= CLUSTER_KM
                       for _, b, _, _ in best_of)
            if near:
                picks = [(anchor[0], anchor[2], anchor[3])]
            else:
                out["candidates"] = sorted({i for _, c, _ in picks for i in c})
                out["match"] = "ambiguous"
                out["matched_text"] = "; ".join(t for t, _, _ in picks)
                return out

        t, cands, dropped = picks[0]
        # GeoNames often lists a city and its municipality as separate entries a
        # few km apart: entries within CLUSTER_KM of the top-ranked one are one place.
        rank = lambda i: (g.p_code[i] in ("PPLC", "PPLA"), g.p_pop[i])
        top = max(cands, key=rank)
        far = [i for i in cands if i != top and
               haversine_km(g.p_lat[i], g.p_lon[i], g.p_lat[top], g.p_lon[top]) > CLUSTER_KM]
        cands = [top] + far
        out["matched_text"], out["candidates"], out["region_dropped"] = t, cands, dropped
        if len(cands) == 1:
            out["match"] = "unique"
        else:
            # A homonym wins if it is the only capital (national or first-order)
            # and also the most populous, or DOMINANCE times larger than the next.
            capitals = [i for i in cands if g.p_code[i] in ("PPLC", "PPLA")]
            pops = sorted((g.p_pop[i] for i in cands), reverse=True)
            dominant = (len(capitals) == 1 and capitals[0] == top and g.p_pop[top] == pops[0]) or \
                       (pops[0] >= DOMINANCE * max(pops[1], 1) and pops[0] >= 10000)
            out["match"] = "dominant" if dominant else "ambiguous"
        return out

    # -- full annotation ---------------------------------------------------
    def resolve(self, raw: str, country_text: str, country_source: str = "biosample",
                lat: Optional[float] = None, lon: Optional[float] = None) -> Dict[str, Any]:
        g = self.g
        text = self.resolve_text(raw, country_text, country_source)
        has_xy = lat is not None and lon is not None and not (lat == 0 and lon == 0)
        dec = min(coord_decimals(lat), coord_decimals(lon)) if has_xy else 0
        precise = has_xy and dec >= 1          # whole-degree coordinates only bound the country
        ann: Dict[str, Any] = {"country_iso2": text["country"], "admin1_code": text["admin1"], "place_index": None,
                               "status": "unresolved", "country_status": "unresolved", "method": None,
                               "distance_km": None, "matched_text": text["matched_text"], "note": None,
                               "coord_decimals": dec if has_xy else None}

        def nearest(km, iso2=None):
            return self._cached("n", g.nearest, lat, lon, km, iso2)

        def largest(km, iso2=None):
            return self._cached("l", g.largest_within, lat, lon, km, iso2)

        # Country.
        if text["country"]:
            ann["country_status"] = "probable" if text["country_loose"] else "verified"
            if precise and nearest(BORDER_KM, text["country"])[0] is None:
                other, _ = nearest(BORDER_KM)
                if other is not None:
                    ann["country_status"] = "conflicting"
                    ann["note"] = f"coordinates near {g.p_name[other]} ({g.p_country[other]})"
        elif precise:
            i, _ = nearest(BORDER_KM)
            if i is not None:
                ann["country_iso2"], ann["country_status"], ann["method"] = g.p_country[i], "probable", "coordinates"

        if ann["country_status"] == "conflicting":
            ann["admin1_code"] = None
            return ann

        # Place from text, checked against precise coordinates.
        cands, match = text["candidates"], text["match"]
        strong = match == "unique" and not text["region_dropped"] and not text.get("region_city")
        decided = False
        if cands:
            if precise:
                near = min(cands, key=lambda i: haversine_km(lat, lon, g.p_lat[i], g.p_lon[i]))
                d = haversine_km(lat, lon, g.p_lat[near], g.p_lon[near])
                if d <= verify_radius_km(g.p_pop[near], dec):
                    ann.update(place_index=near, status="verified", method="text+coordinates", distance_km=round(d, 1))
                    decided = True
                elif strong:
                    ann.update(status="conflicting", method="text", distance_km=round(d, 1),
                               note=f"'{text['matched_text']}' is {d:.0f} km from the reported coordinates")
                    decided = True
                # weak text match far from the coordinates: fall through to the coordinates
            elif match == "unique":
                ann.update(place_index=cands[0], method="text",
                           status="verified" if strong else "probable",
                           note=None if strong else ("region's main city" if text.get("region_city")
                                                     else "region did not contain the place"))
                decided = True
            elif match == "dominant":
                ann.update(place_index=max(cands, key=lambda i: g.p_pop[i]), status="probable", method="text-population",
                           note=f"largest of {len(cands)} places named '{text['matched_text']}'")
                decided = True
            else:
                ann.update(status="ambiguous", method="text", note=f"{len(cands)} places named '{text['matched_text']}'")
                decided = True

        # City-states: the country is the city.
        if not decided and ann["country_iso2"] in CITY_STATES and ann["country_status"] in ("verified", "probable"):
            cap = g.capital(ann["country_iso2"])
            if cap is not None:
                ann.update(place_index=cap, status="probable", method="city-state")
                decided = True

        # Place from precise coordinates alone: the most populous place nearby.
        if not decided and precise and ann["country_iso2"]:
            i, d = largest(COORD_KM, ann["country_iso2"])
            if i is not None:
                ann.update(place_index=i, status="probable", method="coordinates", distance_km=round(d, 1),
                           note=ann["note"] or (f"text '{text['matched_text']}' did not match nearby places"
                                                if cands else None))
            elif cands and match == "ambiguous":
                ann.update(status="ambiguous", method="text", note=f"{len(cands)} places named '{text['matched_text']}'")

        if ann["place_index"] is not None:
            i = ann["place_index"]
            if g.p_country[i] != ann["country_iso2"]:      # never store a place outside its country
                ann.update(place_index=None, status="conflicting", note="matched place lies in another country")
            elif g.p_admin1[i]:
                ann["admin1_code"] = g.p_admin1[i]
        return ann
