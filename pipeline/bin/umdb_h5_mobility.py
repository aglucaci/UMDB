#!/usr/bin/env python3
"""H5 · Are cities linked by air travel more alike microbially than distance and climate predict?

Within each habitat with >= --min-cities cities, builds a mean (centroid) species profile per
city and the Bray–Curtis dissimilarity between cities. Air connectivity is the number of
direct OpenFlights routes between their airports (matched within 50 km), expressed as a
distance: -log(1 + routes). Partial Mantel tests (Spearman):
  community dissimilarity ~ air distance | great-circle distance
  community dissimilarity ~ air distance | climate difference (|Δ latitude|)

Decision rule: "supported" when the partial Mantel r (controlling great-circle distance)
is > 0 with p < 0.05 in the habitat with the most cities. Needs --routes.
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import (base_parser, bray_curtis, load_ecology, mantel, pairwise_km, parse_args, read_tsv,  # noqa: E402
                         skip, write_summary, write_table)

TITLE = "Human mobility and microbial biogeography"
QUESTION = "Are cities connected by air travel more similar microbially than geography and climate predict?"


def main():
    p = base_parser(__doc__)
    p.add_argument("--routes", help="city_routes.tsv from umdb_covariates.py")
    p.add_argument("--min-cities", type=int, default=8)
    a = parse_args(p)
    routes = read_tsv(a.routes)
    if routes is None:
        skip(a.outdir, a.id, TITLE, QUESTION, "No OpenFlights routes (pass --openflights_dir).")
    X, meta = load_ecology(a)
    if X is None:
        skip(a.outdir, a.id, TITLE, QUESTION, "No species profiles.")
    rt = {}
    for r in routes.itertuples():
        rt[(str(r.place_a), str(r.place_b))] = rt[(str(r.place_b), str(r.place_a))] = r.routes
    results, rows = {}, []
    for hab, g in meta.groupby("urban_context"):
        cities = g.groupby("place_id").agg(lat=("place_lat", "first"), lon=("place_lon", "first"))
        if len(cities) < a.min_cities:
            continue
        R = X.loc[g.index]
        R = R.div(R.sum(axis=1).replace(0, 1), axis=0)
        cent = R.groupby(g["place_id"]).mean().loc[cities.index]
        Dm = bray_curtis(cent.to_numpy())
        ids = list(cities.index)
        A = np.array([[0.0 if i == j else -np.log1p(rt.get((i, j), 0)) for j in ids] for i in ids])
        Dg = pairwise_km(cities["lat"].to_numpy(float), cities["lon"].to_numpy(float))
        lat = cities["lat"].abs().to_numpy(float)
        Dc = np.abs(lat[:, None] - lat[None, :])
        res = {"cities": len(ids), "connected_pairs": int((A < 0).sum() // 2),
               "mantel_air": mantel(Dm, A, a.permutations, a.seed),
               "partial_air_given_distance": mantel(Dm, A, a.permutations, a.seed, D3=Dg),
               "partial_air_given_climate": mantel(Dm, A, a.permutations, a.seed, D3=Dc),
               "mantel_distance": mantel(Dm, Dg, a.permutations, a.seed)}
        results[hab] = res
        rows.append({"habitat": hab, **{k: v["r"] for k, v in res.items() if isinstance(v, dict)},
                     **{k + "_p": v["p"] for k, v in res.items() if isinstance(v, dict)}})
    if not results:
        skip(a.outdir, a.id, TITLE, QUESTION, f"No habitat sampled in >= {a.min_cities} cities.")
    main_hab = max(results, key=lambda h: results[h]["cities"])
    pm = results[main_hab]["partial_air_given_distance"]
    files = [write_table(pd.DataFrame(rows), a.outdir, "mantel_by_habitat.tsv")]
    write_summary(a.outdir, a.id, TITLE, QUESTION,
                  "supported" if pm["r"] > 0 and pm["p"] < 0.05 else "not_supported",
                  {"by_habitat": results, "primary_habitat": main_hab},
                  interpretation=(f"{main_hab.replace('_', ' ')} ({results[main_hab]['cities']} cities): partial Mantel r "
                                  f"= {pm['r']:.3f} (p={pm['p']:.3g}) for air connectivity given distance."),
                  caveats=["OpenFlights routes are a static snapshot; passenger volume, rail, commuting and "
                           "trade are not yet included.",
                           "Shared taxa do not establish transport by travellers; this is a correlation test.",
                           "Few cities per habitat give low power; Mantel tests are conservative."],
                  files=files)


if __name__ == "__main__":
    main()
