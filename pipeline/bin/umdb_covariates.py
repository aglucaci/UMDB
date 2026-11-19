#!/usr/bin/env python3
"""Versioned external covariates for UMDB samples, countries and cities.

Sources (each recorded in covariates_manifest.json with retrieval date and SHA-256):
  GeoNames countryInfo  population, area, continent            (always; shipped with the catalog)
  GeoNames cities500    populated places >= --min-city-pop      (always; blind-spot analysis)
  World Bank WDI        GDP per capita, urban population share,
                        safely managed sanitation, R&D spending (--worldbank TSV, or --fetch-worldbank)
  OpenFlights           airports and routes -> city air connectivity (--openflights-dir, optional)
  Köppen–Geiger         climate class at each place (--koppen-raster + rasterio, optional;
                        otherwise a latitude-band proxy, labelled as such)

Not yet integrated (planned, see pipeline/README.md): WorldPop, GHSL, ERA5, OpenAQ,
OpenStreetMap, WHO/UNICEF JMP at sub-national resolution, MODIS.
"""
import argparse
import hashlib
import io
import json
import os
import sys
import urllib.request
import zipfile
from datetime import datetime, timezone

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import climate_band, haversine_km  # noqa: E402

WB_INDICATORS = {
    "NY.GDP.PCAP.KD": "gdp_per_capita_usd2015",
    "SP.URB.TOTL.IN.ZS": "urban_population_pct",
    "SH.STA.SMSS.ZS": "sanitation_safely_managed_pct",
    "GB.XPD.RSDV.GD.ZS": "rd_expenditure_pct_gdp",
    "SP.POP.TOTL": "population_wb",
}
COUNTRY_COLS = ["iso2", "iso3", "iso_numeric", "fips", "name", "capital", "area_km2", "population",
                "continent"]
CITY_COLS = ["geonames_id", "name", "asciiname", "alternatenames", "lat", "lon", "feature_class",
             "feature_code", "country_iso2", "cc2", "admin1", "admin2", "admin3", "admin4",
             "population", "elevation", "dem", "timezone", "modified"]


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for b in iter(lambda: fh.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def read_country_info(path):
    rows = [ln.rstrip("\n").split("\t") for ln in open(path, encoding="utf-8") if not ln.startswith("#")]
    df = pd.DataFrame([r[:9] for r in rows if len(r) >= 9], columns=COUNTRY_COLS)
    df["population"] = pd.to_numeric(df["population"], errors="coerce")
    df["area_km2"] = pd.to_numeric(df["area_km2"], errors="coerce")
    return df[["iso2", "iso3", "name", "continent", "population", "area_km2"]]


def read_cities(path, min_pop):
    with zipfile.ZipFile(path) as z:
        name = [n for n in z.namelist() if n.endswith(".txt")][0]
        with z.open(name) as fh:
            df = pd.read_csv(fh, sep="\t", header=None, names=CITY_COLS, quoting=3,
                             usecols=["geonames_id", "name", "lat", "lon", "feature_code",
                                      "country_iso2", "population"], low_memory=False)
    return df[df["population"] >= min_pop].reset_index(drop=True)


def fetch_worldbank(start, end):
    frames = []
    for code, col in WB_INDICATORS.items():
        url = (f"https://api.worldbank.org/v2/country/all/indicator/{code}"
               f"?format=json&per_page=20000&date={start}:{end}")
        with urllib.request.urlopen(url, timeout=60) as r:
            payload = json.load(r)
        rows = payload[1] if len(payload) > 1 and payload[1] else []
        frames.append(pd.DataFrame([{"iso3": x["countryiso3code"], "year": int(x["date"]),
                                     "indicator": col, "value": x["value"]}
                                    for x in rows if x.get("countryiso3code")]))
    return pd.concat(frames, ignore_index=True)


def latest_per_country(wb):
    """Most recent non-null value of each indicator per country (long -> wide)."""
    wb = wb.dropna(subset=["value"]).sort_values("year")
    last = wb.groupby(["iso3", "indicator"]).tail(1)
    wide = last.pivot(index="iso3", columns="indicator", values="value")
    years = last.pivot(index="iso3", columns="indicator", values="year").add_suffix("_year")
    return wide.join(years).reset_index()


def openflights(dir_, places):
    """Airports matched to the nearest UMDB place (<= 50 km); route counts between places."""
    ap = pd.read_csv(os.path.join(dir_, "airports.dat"), header=None, quotechar='"',
                     names=["id", "name", "city", "country", "iata", "icao", "lat", "lon", "alt", "tz",
                            "dst", "tzdb", "type", "source"], na_values=["\\N"])
    rt = pd.read_csv(os.path.join(dir_, "routes.dat"), header=None,
                     names=["airline", "airline_id", "src", "src_id", "dst", "dst_id", "codeshare",
                            "stops", "equipment"], na_values=["\\N"])
    ap = ap.dropna(subset=["lat", "lon"])
    plat, plon = places["lat"].to_numpy(), places["lon"].to_numpy()
    match = {}
    for row in ap.itertuples():
        d = haversine_km(row.lat, row.lon, plat, plon)
        i = int(np.argmin(d))
        if d[i] <= 50:
            match[int(row.id)] = int(places["place_id"].iloc[i])
    rt = rt.dropna(subset=["src_id", "dst_id"])
    rt["a"] = rt["src_id"].astype(int).map(match)
    rt["b"] = rt["dst_id"].astype(int).map(match)
    rt = rt.dropna(subset=["a", "b"])
    rt = rt[rt["a"] != rt["b"]]
    rt[["a", "b"]] = np.sort(rt[["a", "b"]].astype(int).to_numpy(), axis=1)
    pairs = rt.groupby(["a", "b"]).size().reset_index(name="routes")
    pairs.columns = ["place_a", "place_b", "routes"]
    deg = pd.concat([pairs[["place_a", "routes"]].rename(columns={"place_a": "place_id"}),
                     pairs[["place_b", "routes"]].rename(columns={"place_b": "place_id"})])
    deg = deg.groupby("place_id")["routes"].agg(["sum", "count"]).rename(
        columns={"sum": "air_routes", "count": "air_partner_cities"})
    n_air = pd.Series(match).value_counts().rename("airports")
    return pairs, deg.join(n_air, how="outer").fillna(0).reset_index().rename(columns={"index": "place_id"})


def koppen(places, raster):
    try:
        import rasterio  # noqa: F401
    except ImportError:
        return None
    import rasterio
    with rasterio.open(raster) as src:
        vals = [v[0] for v in src.sample(zip(places["lon"], places["lat"]))]
    return pd.Series(vals, index=places.index, name="koppen_code")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--samples", required=True)
    ap.add_argument("--geonames-dir", required=True)
    ap.add_argument("--worldbank", help="pre-fetched World Bank long TSV (iso3, year, indicator, value)")
    ap.add_argument("--fetch-worldbank", action="store_true")
    ap.add_argument("--wb-years", default="2010:2024")
    ap.add_argument("--openflights-dir")
    ap.add_argument("--koppen-raster")
    ap.add_argument("--min-city-pop", type=int, default=100000)
    ap.add_argument("--outdir", default=".")
    a = ap.parse_args()
    os.makedirs(a.outdir, exist_ok=True)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest = {"generated_utc": now, "sources": {}}

    ci_path = os.path.join(a.geonames_dir, "countryInfo.txt")
    cities_path = os.path.join(a.geonames_dir, "cities500.zip")
    countries = read_country_info(ci_path)
    manifest["sources"]["geonames_countryInfo"] = {"file": ci_path, "sha256": sha256_file(ci_path)}

    # ---- World Bank
    wb = None
    if a.worldbank and os.path.exists(a.worldbank):
        wb = pd.read_csv(a.worldbank, sep="\t", keep_default_na=False, na_values=[""])
        manifest["sources"]["worldbank"] = {"file": a.worldbank, "sha256": sha256_file(a.worldbank)}
    elif a.fetch_worldbank:
        s, e = a.wb_years.split(":")
        try:
            wb = fetch_worldbank(int(s), int(e))
            path = os.path.join(a.outdir, "worldbank_wdi.tsv")
            wb.to_csv(path, sep="\t", index=False)
            manifest["sources"]["worldbank"] = {"api": "https://api.worldbank.org/v2", "retrieved_utc": now,
                                                "indicators": WB_INDICATORS, "years": a.wb_years,
                                                "file": "worldbank_wdi.tsv", "sha256": sha256_file(path)}
        except Exception as exc:  # offline runs still produce GeoNames covariates
            print(f"World Bank fetch failed ({exc}); continuing without it", file=sys.stderr)
            manifest["sources"]["worldbank"] = {"error": str(exc)}
    if wb is not None and len(wb):
        countries = countries.merge(latest_per_country(wb), on="iso3", how="left")
    countries.to_csv(os.path.join(a.outdir, "country_covariates.tsv"), sep="\t", index=False)

    # ---- cities (blind spots) and UMDB places
    cities = read_cities(cities_path, a.min_city_pop)
    cities.to_csv(os.path.join(a.outdir, "cities.tsv"), sep="\t", index=False)
    manifest["sources"]["geonames_cities500"] = {"file": cities_path, "sha256": sha256_file(cities_path),
                                                 "min_population": a.min_city_pop}

    s = pd.read_csv(a.samples, sep="\t", low_memory=False, keep_default_na=False, na_values=[""],
                    usecols=["place_id", "place_name", "country_iso2", "place_population", "place_lat",
                             "place_lon"])
    places = (s.dropna(subset=["place_id"]).drop_duplicates("place_id")
              .rename(columns={"place_lat": "lat", "place_lon": "lon", "place_population": "population"}))
    places["place_id"] = places["place_id"].astype(int)
    places["abs_lat"] = places["lat"].abs()
    places["hemisphere"] = np.where(places["lat"] >= 0, "N", "S")
    places["climate_band"] = places["lat"].map(climate_band)
    k = koppen(places, a.koppen_raster) if a.koppen_raster else None
    if k is not None:
        places["koppen_code"] = k
        manifest["sources"]["koppen"] = {"file": a.koppen_raster, "sha256": sha256_file(a.koppen_raster)}
    else:
        manifest["sources"]["climate"] = {"method": "latitude band proxy (tropical <23.44°, subtropical <35°, "
                                                    "temperate <60°, boreal/polar)"}
    if a.openflights_dir:
        pairs, deg = openflights(a.openflights_dir, places.reset_index(drop=True))
        places = places.merge(deg, on="place_id", how="left").fillna({"air_routes": 0, "air_partner_cities": 0,
                                                                      "airports": 0})
        pairs.to_csv(os.path.join(a.outdir, "city_routes.tsv"), sep="\t", index=False)
        manifest["sources"]["openflights"] = {
            f: sha256_file(os.path.join(a.openflights_dir, f)) for f in ("airports.dat", "routes.dat")}
    places.to_csv(os.path.join(a.outdir, "city_covariates.tsv"), sep="\t", index=False)

    manifest["rows"] = {"countries": len(countries), "cities_ge_min_pop": len(cities), "umdb_places": len(places)}
    with open(os.path.join(a.outdir, "covariates_manifest.json"), "w") as fh:
        json.dump(manifest, fh, indent=2)
        fh.write("\n")
    print(json.dumps(manifest["rows"]))


if __name__ == "__main__":
    main()
