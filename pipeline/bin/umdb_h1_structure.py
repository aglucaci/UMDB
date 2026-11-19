#!/usr/bin/env python3
"""H1 · Does urban habitat explain more community variation than geography?

On Bray–Curtis dissimilarities of species profiles (stratified subsample of at most
--max-samples, balanced over habitat x city):

1. Variance partitioning (distance-based RDA, adjusted R²) among habitat, city and
   BioProject: unique and shared fractions.
2. PERMANOVA for habitat with permutations restricted within cities (habitat effect
   holding city fixed), and for city restricted within habitats.
3. The direct test, using only pairs from different BioProjects (removes within-study
   batch effects): Δ = mean similarity of same-habitat/different-city pairs minus
   different-habitat/same-city pairs, with a 95% cluster bootstrap over cities.
   (Are subway samples from Tokyo and New York more alike than a New York subway and
   New York wastewater?)
4. Distance decay within habitat across cities: Spearman correlation of similarity
   with log great-circle distance; permutation p shuffling city locations.

Decision rule: "supported" when Δ > 0 with the bootstrap interval above 0 and the
unique habitat fraction exceeds the unique city fraction.
"""
import itertools
import os
import sys

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import (adj_r2, base_parser, bray_curtis, city_bootstrap_weights, dbrda_r2, design,  # noqa: E402
                         gower_centered, haversine_km, load_ecology, pair_frame, pairwise_km, parse_args, pcoa, permanova, plt,
                         skip, stratified_subsample, weighted_pair_mean, write_summary, write_table)

TITLE = "Universal ecological structure of urban microbiomes"
QUESTION = "Does habitat type explain more variation in community composition than the city sampled?"


def varpart(G, meta, sets):
    n = len(meta)
    r2 = {}
    for k in range(1, len(sets) + 1):
        for combo in itertools.combinations(sets, k):
            X = design(meta, list(combo))
            raw, p = dbrda_r2(G, X)
            r2[combo] = adj_r2(raw, n, p)
    a, b, c = sets
    full = r2[(a, b, c)]
    unique = {a: full - r2[(b, c)], b: full - r2[(a, c)], c: full - r2[(a, b)]}
    return r2, unique, full


def delta_stat(pairs, w=None):
    m = ~pairs["same_project"].to_numpy()
    sh_dc = m & pairs["same_habitat"].to_numpy() & ~pairs["same_city"].to_numpy()
    dh_sc = m & ~pairs["same_habitat"].to_numpy() & pairs["same_city"].to_numpy()
    return (1 - weighted_pair_mean(pairs, sh_dc, w)) - (1 - weighted_pair_mean(pairs, dh_sc, w)), sh_dc, dh_sc


def main():
    a = parse_args(base_parser(__doc__))
    X, meta = load_ecology(a)
    if X is None:
        skip(a.outdir, a.id, TITLE, QUESTION, "No species profiles (or fewer than 20 profiled urban samples).")
    meta = stratified_subsample(meta, ["urban_context", "place_id"], a.max_samples, a.seed)
    X = X.loc[meta.index]
    D = bray_curtis(X.to_numpy())
    G = gower_centered(D)
    r2, unique, full = varpart(G, meta, ["urban_context", "place_id", "project"])
    perm_h = permanova(D, meta["urban_context"], a.permutations, strata=meta["place_id"], seed=a.seed)
    perm_c = permanova(D, meta["place_id"], a.permutations, strata=meta["urban_context"], seed=a.seed + 1)

    pairs = pair_frame(meta, D)
    delta, sh_dc, dh_sc = delta_stat(pairs)
    boots = [delta_stat(pairs, w)[0] for w in city_bootstrap_weights(meta["place_id"].unique(), 500, a.seed)]
    boots = np.array([b for b in boots if np.isfinite(b)])
    lo, hi = (np.percentile(boots, [2.5, 97.5]) if boots.size else (np.nan, np.nan))

    # distance decay within habitat, across cities and studies
    km = pairwise_km(meta["place_lat"].to_numpy(float), meta["place_lon"].to_numpy(float))
    pairs["km"] = km[pairs["i"], pairs["j"]]
    dd = pairs[pairs["same_habitat"] & ~pairs["same_city"] & ~pairs["same_project"]]
    decay = {}
    if len(dd) > 30:
        rho = spearmanr(np.log10(dd["km"] + 1), 1 - dd["d"]).statistic
        cities = meta.drop_duplicates("place_id").set_index("place_id")[["place_lat", "place_lon"]]
        rng = np.random.default_rng(a.seed)
        ge = 0
        for _ in range(min(a.permutations, 499)):
            perm = cities.iloc[rng.permutation(len(cities))]
            perm.index = cities.index
            la = perm["place_lat"].to_dict()
            lo_ = perm["place_lon"].to_dict()
            k = haversine_km(dd["city_i"].map(la), dd["city_i"].map(lo_), dd["city_j"].map(la), dd["city_j"].map(lo_))
            ge += spearmanr(np.log10(k + 1), 1 - dd["d"]).statistic <= rho
        decay = {"pairs": len(dd), "spearman_similarity_vs_log_km": rho,
                 "p_one_sided_negative": (ge + 1) / (min(a.permutations, 499) + 1)}

    pc = pcoa(D, 2)
    files = [write_table(pd.DataFrame([{"terms": "+".join(k), "adj_r2": v} for k, v in r2.items()]), a.outdir,
                         "varpart_adj_r2.tsv"),
             write_table(pd.DataFrame({"biosample": meta.index, "habitat": meta["urban_context"],
                                       "place_id": meta["place_id"], "PCo1": pc[:, 0], "PCo2": pc[:, 1]}),
                         a.outdir, "pcoa.tsv")]
    pyplot = plt()
    if pyplot:
        fig, ax = pyplot.subplots(1, 3, figsize=(11, 3.4))
        for h, g in meta.assign(x=pc[:, 0], y=pc[:, 1]).groupby("urban_context"):
            ax[0].scatter(g["x"], g["y"], s=4, alpha=0.6, label=h.replace("_", " "))
        ax[0].legend(frameon=False, fontsize=6, markerscale=2)
        ax[0].set(xlabel="PCo1", ylabel="PCo2", title="Bray–Curtis PCoA")
        ax[1].bar(["habitat", "city", "study"], [unique["urban_context"], unique["place_id"], unique["project"]],
                  color="#2a6f97")
        ax[1].set(ylabel="Unique adjusted R²", title="Variance partitioning")
        ax[2].boxplot([1 - pairs.loc[sh_dc, "d"], 1 - pairs.loc[dh_sc, "d"]], showfliers=False)
        ax[2].set_xticks([1, 2], ["same habitat\ndifferent city", "different habitat\nsame city"])
        ax[2].set(ylabel="Bray–Curtis similarity", title=f"Δ = {delta:.3f}")
        fig.tight_layout()
        fig.savefig(os.path.join(a.outdir, "structure.png"))
        files.append("structure.png")

    ok = np.isfinite(lo) and lo > 0 and unique["urban_context"] > unique["place_id"]
    not_ok = np.isfinite(hi) and hi < 0
    status = "supported" if ok else ("not_supported" if not_ok else "inconclusive")
    res = {"samples": len(meta), "habitats": meta["urban_context"].nunique(), "cities": meta["place_id"].nunique(),
           "bioprojects": meta["project"].nunique(), "features": X.shape[1],
           "varpart_unique_adj_r2": {"habitat": unique["urban_context"], "city": unique["place_id"],
                                     "study": unique["project"]}, "varpart_total_adj_r2": full,
           "permanova_habitat_within_city": perm_h, "permanova_city_within_habitat": perm_c,
           "delta_similarity": delta, "delta_ci95": [lo, hi],
           "pairs_same_habitat_diff_city": int(sh_dc.sum()), "pairs_diff_habitat_same_city": int(dh_sc.sum()),
           "distance_decay": decay}
    write_summary(a.outdir, a.id, TITLE, QUESTION, status, res,
                  interpretation=(f"Same-habitat pairs from different cities are {delta:+.3f} more similar than "
                                  f"different-habitat pairs from the same city (95% CI {lo:.3f} to {hi:.3f}); "
                                  f"unique R² habitat {unique['urban_context']:.3f} vs city {unique['place_id']:.3f}."),
                  caveats=["City and BioProject are largely nested; their shared fraction cannot be split.",
                           "Reference-based profiles miss uncharacterised taxa, which may be more city-specific.",
                           "Profiles are from depth-capped reads; rare taxa are under-detected equally across samples."],
                  files=files)


if __name__ == "__main__":
    main()
