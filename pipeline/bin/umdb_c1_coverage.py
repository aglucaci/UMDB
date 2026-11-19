#!/usr/bin/env python3
"""C1 · Global coverage inequality and the blind spots of urban genomic surveillance.

Urban BioSamples with a verified or probable country are counted per country and
compared with population (GeoNames countryInfo). Reports the Gini coefficient of
samples across countries, the population-weighted concentration index of samples
per capita (0 = every person equally covered, 1 = all samples cover nobody's share),
continent shares of samples vs people, and the large cities (>= 1 M people) with no
urban sample within --blind-km of their centre.

Decision rule (fixed in advance): inequality is "supported" when the concentration
index is >= 0.4 and its 95% bootstrap interval (resampling countries) excludes 0.4.
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import (CONTINENTS, base_parser, gini, haversine_km, load_samples, parse_args, plt,  # noqa: E402
                         read_tsv, write_summary, write_table)

TITLE = "Global blind spots of urban genomic surveillance"
QUESTION = "Is urban sequencing coverage geographically unequal relative to where people live?"
THRESHOLD = 0.4


def concentration_index(samples, pop):
    """Area between the line of equality and the sample-vs-population concentration curve, x2."""
    df = pd.DataFrame({"s": samples, "p": pop}).dropna()
    df = df[df["p"] > 0]
    df["rate"] = df["s"] / df["p"]
    df = df.sort_values("rate")
    x = np.concatenate([[0], np.cumsum(df["p"]) / df["p"].sum()])
    y = np.concatenate([[0], np.cumsum(df["s"]) / max(1, df["s"].sum())])
    return float(1 - 2 * np.trapezoid(y, x)), x, y


def main():
    p = base_parser(__doc__)
    p.add_argument("--blind-km", type=float, default=50)
    p.add_argument("--min-country-pop", type=float, default=1e6)
    a = parse_args(p)
    s = load_samples(a.samples)
    cov = read_tsv(a.country_covariates)
    s = s[s["country_status"].isin(["verified", "probable"])]

    n = s.groupby("country_iso2").size().rename("urban_samples")
    c = cov.set_index("iso2")[["name", "continent", "population"]].join(n).fillna({"urban_samples": 0})
    c = c[c["population"] >= a.min_country_pop]
    c["samples_per_million"] = c["urban_samples"] / c["population"] * 1e6
    ci, x, y = concentration_index(c["urban_samples"], c["population"])
    rng = np.random.default_rng(a.seed)
    boots = []
    for _ in range(1000):
        b = c.iloc[rng.integers(0, len(c), len(c))]
        boots.append(concentration_index(b["urban_samples"], b["population"])[0])
    lo, hi = np.percentile(boots, [2.5, 97.5])

    cont = c.groupby("continent")[["urban_samples", "population"]].sum()
    cont["share_samples"] = cont["urban_samples"] / cont["urban_samples"].sum()
    cont["share_population"] = cont["population"] / cont["population"].sum()
    cont["representation_ratio"] = cont["share_samples"] / cont["share_population"]
    cont.index = [CONTINENTS.get(k, k) for k in cont.index]

    # ---- blind spots: large cities with no urban sample nearby
    cities = read_tsv(a.cities)
    big = cities[cities["population"] >= 1_000_000].copy()
    sites = s.dropna(subset=["place_lat", "place_lon"]).drop_duplicates("place_id")
    near = []
    for row in big.itertuples():
        d = haversine_km(row.lat, row.lon, sites["place_lat"].to_numpy(), sites["place_lon"].to_numpy())
        near.append(int((d <= a.blind_km).sum()))
    big["umdb_places_within_km"] = near
    blind = big[big["umdb_places_within_km"] == 0].sort_values("population", ascending=False)
    blind_cont = blind.merge(cov[["iso2", "continent"]], left_on="country_iso2", right_on="iso2", how="left")

    files = [write_table(c.reset_index().rename(columns={"index": "iso2"}).sort_values("urban_samples",
                                                                                      ascending=False),
                         a.outdir, "country_coverage.tsv"),
             write_table(cont.reset_index().rename(columns={"index": "continent"}), a.outdir,
                         "continent_coverage.tsv"),
             write_table(blind[["geonames_id", "name", "country_iso2", "population", "lat", "lon"]],
                         a.outdir, "blind_spot_cities.tsv")]
    pyplot = plt()
    if pyplot:
        fig, ax = pyplot.subplots(1, 2, figsize=(9, 3.6))
        ax[0].plot(x, y, color="#2a6f97")
        ax[0].plot([0, 1], [0, 1], color="#999", ls="--", lw=0.8)
        ax[0].set(xlabel="Cumulative share of population", ylabel="Cumulative share of urban samples",
                  title=f"Concentration index {ci:.2f}")
        cont_sorted = cont.sort_values("representation_ratio")
        ax[1].barh(cont_sorted.index, cont_sorted["representation_ratio"], color="#2a6f97")
        ax[1].axvline(1, color="#999", ls="--", lw=0.8)
        ax[1].set(xscale="log", xlabel="Share of samples / share of population")
        fig.tight_layout()
        fig.savefig(os.path.join(a.outdir, "coverage.png"))
        files.append("coverage.png")

    status = "supported" if (ci >= THRESHOLD and lo > THRESHOLD) else (
        "not_supported" if hi < THRESHOLD else "inconclusive")
    res = {"countries_ge_1M": len(c), "countries_with_samples": int((c["urban_samples"] > 0).sum()),
           "gini_samples_across_countries": gini(c["urban_samples"]),
           "concentration_index": ci, "concentration_index_ci95": [lo, hi],
           "top5_countries_share": float(c["urban_samples"].nlargest(5).sum() / c["urban_samples"].sum()),
           "cities_ge_1M": len(big), "blind_spot_cities_ge_1M": len(blind),
           "blind_spot_population": float(blind["population"].sum()),
           "blind_spots_by_continent": blind_cont["continent"].map(lambda k: CONTINENTS.get(k, k))
           .value_counts().to_dict(),
           "representation_ratio_by_continent": cont["representation_ratio"].round(3).to_dict()}
    write_summary(a.outdir, a.id, TITLE, QUESTION, status, res,
                  interpretation=(f"Concentration index {ci:.2f} (95% CI {lo:.2f}–{hi:.2f}); "
                                  f"{len(blind)} of {len(big)} cities of ≥1 M people have no urban sample within "
                                  f"{a.blind_km:.0f} km."),
                  caveats=["Counts only samples with a validated country; ~half the catalog has no resolved place.",
                           "Coverage is what is public in INSDC, not what is sequenced; national databases "
                           "and unreleased surveillance data are invisible here.",
                           "City population is the GeoNames place population, not the metropolitan area."],
                  files=files)


if __name__ == "__main__":
    main()
