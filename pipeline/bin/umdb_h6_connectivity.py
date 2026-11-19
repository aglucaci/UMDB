#!/usr/bin/env python3
"""H6 · Do a city's habitats share microbial taxa in non-random patterns?

In each city with >= 2 habitats of >= --min-per-habitat samples, pools species presence
per habitat (each habitat downsampled to the same number of samples so sampling effort
does not drive sharing) and computes Jaccard similarity for every habitat pair.
Null: habitat labels permuted among the city's samples (--null permutations); z-score
of the observed similarity. Habitat-pair edges summarise mean similarity, mean z and
the share of cities where z > 2 (more sharing than chance) or z < -2 (less).

"Non-random" is tested two ways: (1) do habitats share fewer taxa than random
assignment would give (mean z < 0, sign test across cities), and (2) do habitat
pairs differ in connectivity (Kruskal–Wallis on z across pairs).

Decision rule: "supported" when both tests give p < 0.05.
Shared taxa alone do not show the direction of exchange.
"""
import itertools
import os
import sys

import numpy as np
import pandas as pd
from scipy.stats import binomtest, kruskal

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import base_parser, load_ecology, parse_args, plt, skip, write_summary, write_table  # noqa: E402

TITLE = "Connectivity between a city's microbial habitats"
QUESTION = "Do wastewater, air, transit, soil and buildings in a city share taxa in non-random patterns?"


def jacc(a, b):
    u = np.logical_or(a, b).sum()
    return np.logical_and(a, b).sum() / u if u else np.nan


def main():
    p = base_parser(__doc__)
    p.add_argument("--min-per-habitat", type=int, default=3)
    p.add_argument("--null", type=int, default=199)
    a = parse_args(p)
    X, meta = load_ecology(a)
    if X is None:
        skip(a.outdir, a.id, TITLE, QUESTION, "No species profiles.")
    P = (X > 0).to_numpy()
    pos = {s: i for i, s in enumerate(X.index)}
    rng = np.random.default_rng(a.seed)
    rows = []
    for city, g in meta.groupby("place_id"):
        counts = g["urban_context"].value_counts()
        habs = counts[counts >= a.min_per_habitat].index
        if len(habs) < 2:
            continue
        k = int(counts[habs].min())
        g = g[g["urban_context"].isin(habs)]
        idx = np.array([pos[s] for s in g.index])
        labels = g["urban_context"].to_numpy()

        def pooled(lab):
            out = {}
            for h in habs:
                members = idx[lab == h]
                members = rng.choice(members, size=k, replace=False)
                out[h] = P[members].any(axis=0)
            return out

        obs = pooled(labels)
        null = {pair: [] for pair in itertools.combinations(sorted(habs), 2)}
        for _ in range(a.null):
            pl = pooled(rng.permutation(labels))
            for h1, h2 in null:
                null[(h1, h2)].append(jacc(pl[h1], pl[h2]))
        for h1, h2 in null:
            o = jacc(obs[h1], obs[h2])
            nv = np.array(null[(h1, h2)])
            sd = nv.std()
            rows.append({"place_id": city, "place_name": g["place_name"].iloc[0], "habitat_a": h1, "habitat_b": h2,
                         "k_per_habitat": k, "jaccard": o, "null_mean": nv.mean(),
                         "z": (o - nv.mean()) / sd if sd > 0 else np.nan})
    if not rows:
        skip(a.outdir, a.id, TITLE, QUESTION, "No city has two habitats with enough samples.")
    city_pairs = pd.DataFrame(rows)
    edges = city_pairs.groupby(["habitat_a", "habitat_b"]).agg(
        cities=("place_id", "nunique"), mean_jaccard=("jaccard", "mean"), mean_z=("z", "mean"),
        share_more_than_chance=("z", lambda z: float((z > 2).mean())),
        share_less_than_chance=("z", lambda z: float((z < -2).mean()))).reset_index()
    z = city_pairs["z"].dropna()
    sign = binomtest(int((z < 0).sum()), len(z), 0.5)
    groups = [g["z"].dropna().to_numpy() for _, g in city_pairs.groupby(["habitat_a", "habitat_b"])
              if g["z"].notna().sum() >= 2]
    kw = kruskal(*groups) if len(groups) >= 2 else None
    files = [write_table(city_pairs, a.outdir, "city_habitat_pairs.tsv"), write_table(edges, a.outdir, "edges.tsv")]
    pyplot = plt()
    if pyplot:
        habs = sorted(set(edges["habitat_a"]) | set(edges["habitat_b"]))
        M = pd.DataFrame(np.nan, index=habs, columns=habs)
        for r in edges.itertuples():
            M.loc[r.habitat_a, r.habitat_b] = M.loc[r.habitat_b, r.habitat_a] = r.mean_z
        fig, ax = pyplot.subplots(figsize=(4.8, 4))
        im = ax.imshow(M.to_numpy(dtype=float), cmap="RdBu_r", vmin=-8, vmax=8)
        ax.set_xticks(range(len(habs)), [h.replace("_", " ") for h in habs], rotation=40, ha="right", fontsize=7)
        ax.set_yticks(range(len(habs)), [h.replace("_", " ") for h in habs], fontsize=7)
        fig.colorbar(im, ax=ax, label="Mean z (sharing vs random)")
        fig.tight_layout()
        fig.savefig(os.path.join(a.outdir, "connectivity.png"))
        files.append("connectivity.png")
    ok = sign.pvalue < 0.05 and kw is not None and kw.pvalue < 0.05
    write_summary(a.outdir, a.id, TITLE, QUESTION, "supported" if ok else "not_supported",
                  {"cities": int(city_pairs["place_id"].nunique()), "city_habitat_pairs": len(city_pairs),
                   "mean_z": float(z.mean()), "sign_test_p": sign.pvalue,
                   "kruskal_pairs": None if kw is None else {"H": kw.statistic, "p": kw.pvalue},
                   "edges": edges.set_index(edges["habitat_a"] + "~" + edges["habitat_b"])[
                       ["cities", "mean_jaccard", "mean_z"]].to_dict(orient="index")},
                  interpretation=(f"Across {city_pairs['place_id'].nunique()} cities, habitats share taxa at mean "
                                  f"z = {z.mean():.2f} relative to random assignment; habitat pairs differ "
                                  f"(Kruskal–Wallis p = {kw.pvalue if kw else float('nan'):.2g})."),
                  caveats=["Species-level sharing; strain-level tracking (e.g. inStrain/StrainPhlAn) is needed to "
                           "argue for exchange.",
                           "Habitats in a city are often sampled by different studies at different times."],
                  files=files)


if __name__ == "__main__":
    main()
