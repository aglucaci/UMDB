#!/usr/bin/env python3
"""Build the synthetic fixture used by ``-profile test``.

Writes into pipeline/tests/data/:
  umdb_fixture.sqlite          the tables and columns umdb_snapshot.py reads
  geonames/countryInfo.txt     six countries
  geonames/cities500.zip       twelve cities (eight sampled, four blind spots)
  worldbank.tsv                GDP, urban share, sanitation, R&D (long format)
  openflights/{airports,routes}.dat
  species.tsv, functions.tsv, arg.tsv   simulated profiles (feature x BioSample)

The simulation plants known effects so the tests have an expected direction:
habitat > city > study for species composition; functions converge more strongly
by habitat than species do; richness grows with city population (beta ~ 0.15);
a seasonal signal that follows the local (hemisphere-aligned) season; ARG load
falls with GDP. Deterministic (seeded). Requires numpy and pandas.
"""
import io
import os
import sqlite3
import zipfile
import zlib

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "data")
rng = np.random.default_rng(7)

COUNTRIES = [  # iso2, iso3, name, continent, population, area, gdp_pc, urban%, sanitation%, rd%
    ("US", "USA", "United States", "NA", 331000000, 9629091, 62000, 83, 97, 3.4),
    ("GB", "GBR", "United Kingdom", "EU", 67000000, 244820, 47000, 84, 98, 2.9),
    ("JP", "JPN", "Japan", "AS", 125000000, 377835, 36000, 92, 99, 3.3),
    ("BR", "BRA", "Brazil", "SA", 213000000, 8511965, 9000, 87, 49, 1.2),
    ("ZA", "ZAF", "South Africa", "AF", 60000000, 1219912, 6000, 68, 63, 0.6),
    ("AU", "AUS", "Australia", "OC", 26000000, 7686850, 59000, 86, 94, 1.8),
]
CITIES = [  # geonames_id, name, iso2, lat, lon, population, sampled
    (5128581, "New York City", "US", 40.714, -74.006, 8804190, True),
    (4930956, "Boston", "US", 42.358, -71.060, 675647, True),
    (2643743, "London", "GB", 51.509, -0.126, 8961989, True),
    (1850147, "Tokyo", "JP", 35.690, 139.692, 13960000, True),
    (1853909, "Osaka", "JP", 34.694, 135.502, 2753862, True),
    (3448439, "Sao Paulo", "BR", -23.548, -46.636, 12400232, True),
    (3369157, "Cape Town", "ZA", -33.926, 18.423, 4618000, True),
    (2147714, "Sydney", "AU", -33.868, 151.207, 5312163, True),
    (4887398, "Chicago", "US", 41.850, -87.650, 2746388, False),
    (3405870, "Belem", "BR", -1.456, -48.502, 1499641, False),
    (993800, "Johannesburg", "ZA", -26.202, 28.044, 5635127, False),
    (1857910, "Kyoto", "JP", 35.021, 135.754, 1459640, False),
]
HABITATS = ["municipal_wastewater", "air_dust", "transit", "built_environment"]
N_SPECIES, N_FUNC, N_ARG = 220, 90, 30


def write_geonames():
    d = os.path.join(OUT, "geonames")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, "countryInfo.txt"), "w") as fh:
        fh.write("# synthetic fixture\n#ISO\tISO3\tISO-Numeric\tfips\tCountry\tCapital\tArea\tPopulation\tContinent\n")
        for iso2, iso3, name, cont, pop, area, *_ in COUNTRIES:
            fh.write(f"{iso2}\t{iso3}\t000\t{iso2}\t{name}\t\t{area}\t{pop}\t{cont}\t.x\n")
    buf = io.StringIO()
    for gid, name, iso2, lat, lon, pop, _ in CITIES:
        f = [gid, name, name, "", lat, lon, "P", "PPL", iso2, "", "01", "", "", "", pop, "", 10, "UTC", "2024-01-01"]
        buf.write("\t".join(map(str, f)) + "\n")
    with zipfile.ZipFile(os.path.join(d, "cities500.zip"), "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("cities500.txt", buf.getvalue())


def write_worldbank():
    rows = []
    for iso2, iso3, name, cont, pop, area, gdp, urb, san, rd in COUNTRIES:
        for y in (2022, 2023):
            rows += [(iso3, y, "gdp_per_capita_usd2015", gdp), (iso3, y, "urban_population_pct", urb),
                     (iso3, y, "sanitation_safely_managed_pct", san), (iso3, y, "rd_expenditure_pct_gdp", rd),
                     (iso3, y, "population_wb", pop)]
    pd.DataFrame(rows, columns=["iso3", "year", "indicator", "value"]).to_csv(
        os.path.join(OUT, "worldbank.tsv"), sep="\t", index=False)


def write_openflights():
    d = os.path.join(OUT, "openflights")
    os.makedirs(d, exist_ok=True)
    sampled = [c for c in CITIES if c[6]]
    with open(os.path.join(d, "airports.dat"), "w") as fh:
        for k, (gid, name, iso2, lat, lon, *_r) in enumerate(sampled, 1):
            fh.write(f'{k},"{name} Intl","{name}","{iso2}","A{k:02d}","AA{k:02d}",{lat + 0.05},{lon + 0.05},'
                     f'10,0,"U","UTC","airport","OurAirports"\n')
    hubs = {1, 3, 4}  # New York, London, Tokyo are hubs
    with open(os.path.join(d, "routes.dat"), "w") as fh:
        for i in range(1, len(sampled) + 1):
            for j in range(1, len(sampled) + 1):
                if i != j and (i in hubs or j in hubs or rng.random() < 0.2):
                    for _ in range(1 + 2 * (i in hubs and j in hubs)):
                        fh.write(f"XX,1,A{i:02d},{i},A{j:02d},{j},,0,320\n")


def simulate():
    sampled = [c for c in CITIES if c[6]]
    gdp = {c[0]: c[6] for c in COUNTRIES}
    hab_mu = {h: rng.normal(0, 2.0, N_SPECIES) for h in HABITATS}
    hab_pool = {h: rng.random(N_SPECIES) < 0.55 for h in HABITATS}
    city_mu = {c[0]: rng.normal(0, 0.7, N_SPECIES) for c in sampled}
    season_species = rng.random(N_SPECIES) < 0.25
    hab_fun = {h: rng.normal(0, 1.5, N_FUNC) for h in HABITATS}
    hab_arg = {h: rng.normal(0, 1.0, N_ARG) for h in HABITATS}
    pmax = max(c[5] for c in sampled)

    rows, sp, fn, ar = [], {}, {}, {}
    k = 0
    for gid, cname, iso2, lat, lon, pop, _ in sampled:
        for h in HABITATS:
            if (gid, h) in {(4930956, "transit"), (3369157, "transit")}:
                continue  # not every city samples every habitat
            for r in range(8):
                k += 1
                acc = f"SAMNFIX{k:05d}"
                proj = f"PRJNAFIXC{gid}" if r % 2 == 0 else f"PRJNAFIXH{HABITATS.index(h)}"
                date = pd.Timestamp("2018-01-01") + pd.Timedelta(days=int(rng.integers(0, 6 * 365)))
                doy = date.day_of_year + (182.5 if lat < 0 else 0)
                season = np.cos(2 * np.pi * (doy - 200) / 365.25)
                proj_mu = np.random.default_rng(zlib.crc32(proj.encode())).normal(0, 0.4, N_SPECIES)
                logit = hab_mu[h] + city_mu[gid] + proj_mu + 1.2 * season * season_species
                # richness rises with population: more of the pool is detectable
                detect = 0.12 + 0.20 * np.log10(pop / 1e5) / np.log10(pmax / 1e5)
                present = hab_pool[h] & (rng.random(N_SPECIES) < detect)
                w = np.exp(logit) * present
                counts = rng.multinomial(20000, w / w.sum())
                sp[acc] = counts / counts.sum()
                f = np.exp(hab_fun[h] + rng.normal(0, 0.15, N_FUNC) + 0.1 * city_mu[gid][:N_FUNC])
                fn[acc] = f / f.sum() * 1e6
                a_ = np.exp(hab_arg[h] + 0.8 * np.log(60000 / gdp[iso2]) / 3 + rng.normal(0, 0.3, N_ARG))
                ar[acc] = a_
                rows.append(dict(acc=acc, proj=proj, gid=gid, iso2=iso2, lat=lat, lon=lon, habitat=h,
                                 date=date, tier="confirmed_urban" if r % 3 else "probable_urban"))
    # a few non-urban / uncertain samples, a future date and an unlocated sample for the audit
    extra = [("uncertain", None), ("non_urban", None), ("non_environmental", None)]
    for t, _ in extra * 5:
        k += 1
        rows.append(dict(acc=f"SAMNFIX{k:05d}", proj="PRJNAFIXX", gid=None, iso2=None, lat=None, lon=None,
                         habitat=None, date=pd.Timestamp("2021-06-01"), tier=t))
    rows[0]["date"] = pd.Timestamp("2031-01-01")
    return rows, sp, fn, ar


def write_db(rows):
    path = os.path.join(OUT, "umdb_fixture.sqlite")
    if os.path.exists(path):
        os.remove(path)
    con = sqlite3.connect(path)
    con.executescript("""
    CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
    CREATE TABLE samples (accession TEXT PRIMARY KEY, urban_status TEXT, urban_context TEXT,
        environmental_sample INTEGER, collection_date TEXT, collection_year INTEGER, collection_date_raw TEXT,
        lat REAL, lon REAL, env_broad_scale TEXT, env_local_scale TEXT, env_medium TEXT, isolation_source TEXT);
    CREATE TABLE countries (iso2 TEXT PRIMARY KEY, name TEXT, continent TEXT);
    CREATE TABLE places (geonames_id INTEGER PRIMARY KEY, name TEXT, population INTEGER, lat REAL, lon REAL);
    CREATE TABLE sample_locations (sample_accession TEXT PRIMARY KEY, country_iso2 TEXT, country_status TEXT,
        admin1_code TEXT, place_id INTEGER, status TEXT, coord_decimals INTEGER);
    CREATE TABLE project_samples (project_accession TEXT, sample_accession TEXT);
    CREATE TABLE experiments (accession TEXT PRIMARY KEY, assay_class TEXT, library_strategy TEXT,
        library_source TEXT, library_layout TEXT, platform TEXT, instrument_model TEXT);
    CREATE TABLE runs (accession TEXT PRIMARY KEY, experiment_accession TEXT, sample_accession TEXT,
        project_accession TEXT, release_date TEXT, release_year INTEGER, spots INTEGER, bases INTEGER,
        avg_length INTEGER, center_name TEXT, urban_status TEXT);
    """)
    con.executemany("INSERT INTO meta VALUES (?,?)", [("schema_version", "1.1.0"),
                                                       ("built_utc", "2026-11-16T00:00:00+00:00"),
                                                       ("rules_version", "fixture")])
    con.executemany("INSERT INTO countries VALUES (?,?,?)", [(c[0], c[2], c[3]) for c in COUNTRIES])
    con.executemany("INSERT INTO places VALUES (?,?,?,?,?)",
                    [(c[0], c[1], c[5], c[3], c[4]) for c in CITIES])
    centres = ["BIG GENOME CENTRE"] * 6 + ["UNIVERSITY A", "UNIVERSITY B", "INSTITUTE C", "LAB D"]
    for n, r in enumerate(rows):
        d = r["date"].strftime("%Y-%m-%d")
        rel = (r["date"] + pd.Timedelta(days=400)).strftime("%Y-%m-%d")
        con.execute("INSERT INTO samples VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (r["acc"], r["tier"], r["habitat"], int(r["tier"] != "non_environmental"), d,
                     r["date"].year, d, r["lat"], r["lon"], "urban biome" if n % 2 else None, None, None,
                     "sewage" if r["habitat"] == "municipal_wastewater" else None))
        located = r["gid"] is not None
        con.execute("INSERT INTO sample_locations VALUES (?,?,?,?,?,?,?)",
                    (r["acc"], r["iso2"], "verified" if located else "unresolved", None, r["gid"],
                     "verified" if located else "unresolved", 3 if located else None))
        con.execute("INSERT INTO project_samples VALUES (?,?)", (r["proj"], r["acc"]))
        exp = f"SRXFIX{n:05d}"
        long_read = n % 17 == 0
        con.execute("INSERT INTO experiments VALUES (?,?,?,?,?,?,?)",
                    (exp, "WGS" if n % 5 else "16S", "WGS" if n % 5 else "AMPLICON", "METAGENOMIC", "PAIRED",
                     "OXFORD_NANOPORE" if long_read else "ILLUMINA",
                     "MinION" if long_read else ("Illumina NovaSeq 6000" if r["date"].year >= 2020 else
                                                 "Illumina HiSeq 2500")))
        con.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (f"SRRFIX{n:05d}", exp, r["acc"], r["proj"], rel, int(rel[:4]), 10_000_000,
                     3_000_000_000, 300, centres[n % len(centres)], r["tier"]))
    con.commit()
    con.close()


def write_matrix(d, name, prefix):
    m = pd.DataFrame(d)
    m.index = [f"{prefix}{i:04d}" for i in range(len(m))]
    m.index.name = "feature"
    m = m.loc[m.sum(axis=1) > 0]
    m.round(8).to_csv(os.path.join(OUT, name), sep="\t")


def main():
    os.makedirs(OUT, exist_ok=True)
    write_geonames()
    write_worldbank()
    write_openflights()
    rows, sp, fn, ar = simulate()
    write_db(rows)
    write_matrix(sp, "species.tsv", "s__Synthetic_species_")
    write_matrix(fn, "functions.tsv", "K0")
    write_matrix(ar, "arg.tsv", "argGene")
    print(f"fixture: {len(rows)} samples, {len(sp)} profiled -> {OUT}")


if __name__ == "__main__":
    main()
