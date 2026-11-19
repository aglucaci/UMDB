#!/usr/bin/env python3
"""Select a balanced shotgun-metagenome cohort and audit its feasibility.

Eligible BioSamples: confirmed or probable urban, a habitat, a validated place, no
date anomaly, primary assay in --assays (default WGS), Illumina runs of that assay
totalling >= --min-bases. Selection balances habitats, cities and independent
studies rather than maximising sample count:

  1. the --target is split evenly across habitats (water-filling: a habitat with fewer
     eligible samples than its share gives the remainder to the others);
  2. within a habitat, samples are drawn round-robin across cities, in random order;
  3. no (city, habitat) cell exceeds --max-per-cell and no BioProject exceeds
     --max-per-project.

Writes cohort_samples.tsv, cohort_runs.tsv (the accessions the read track fetches),
feasibility.tsv (eligible vs selected by habitat x continent) and feasibility.json
(download volume before and after depth capping).
"""
import argparse
import json
import os
import sys
from collections import defaultdict

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import CONTINENTS, URBAN_TIERS, read_tsv  # noqa: E402


def water_fill(available: dict, target: int) -> dict:
    alloc = {k: 0 for k in available}
    left, open_ = target, {k for k, v in available.items() if v > 0}
    while left > 0 and open_:
        share = max(1, left // len(open_))
        for k in sorted(open_):
            take = min(share, available[k] - alloc[k], left)
            alloc[k] += take
            left -= take
        open_ = {k for k in open_ if alloc[k] < available[k]}
    return alloc


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--samples", required=True)
    ap.add_argument("--runs", required=True)
    ap.add_argument("--assays", default="WGS")
    ap.add_argument("--platform", default="ILLUMINA")
    ap.add_argument("--min-bases", type=float, default=1e9)
    ap.add_argument("--target", type=int, default=15000)
    ap.add_argument("--max-per-cell", type=int, default=50)
    ap.add_argument("--max-per-project", type=int, default=300)
    ap.add_argument("--subsample-bases", type=float, default=2e9,
                    help="reads are later capped at this depth; used for the volume estimate")
    ap.add_argument("--seed", type=int, default=20261119)
    ap.add_argument("--outdir", default=".")
    a = ap.parse_args()
    os.makedirs(a.outdir, exist_ok=True)
    assays = set(a.assays.split(","))

    s = read_tsv(a.samples)
    r = read_tsv(a.runs)
    r = r[r["assay_class"].isin(assays) & r["platform"].eq(a.platform)]
    rb = r.groupby("biosample").agg(cohort_runs=("run", "size"), cohort_bases=("bases", "sum"))
    s = s.merge(rb, left_on="biosample", right_index=True, how="inner")
    elig = s[s["urban_status"].isin(URBAN_TIERS) & s["urban_context"].notna() & s["has_place"]
             & s["date_ok"] & (s["cohort_bases"] >= a.min_bases)].copy()
    elig["place_id"] = elig["place_id"].astype(int)
    elig["continent_name"] = elig["continent"].map(lambda k: CONTINENTS.get(k, k))

    rng = np.random.default_rng(a.seed)
    elig = elig.iloc[rng.permutation(len(elig))]
    alloc = water_fill(elig["urban_context"].value_counts().to_dict(), a.target)
    per_cell, per_proj, chosen = defaultdict(int), defaultdict(int), []
    for hab, g in elig.groupby("urban_context"):
        queues = {pl: list(gg.index) for pl, gg in g.groupby("place_id")}
        order = list(queues)
        rng.shuffle(order)
        need = alloc.get(hab, 0)
        while need > 0 and order:
            nxt = []
            for pl in order:
                if need == 0:
                    break
                q = queues[pl]
                while q:
                    i = q.pop()
                    row = elig.loc[i]
                    if per_cell[(pl, hab)] >= a.max_per_cell:
                        q.clear()
                        break
                    if per_proj[row["project"]] >= a.max_per_project:
                        continue
                    chosen.append(i)
                    per_cell[(pl, hab)] += 1
                    per_proj[row["project"]] += 1
                    need -= 1
                    break
                if q:
                    nxt.append(pl)
            order = nxt
    cohort = elig.loc[chosen]
    cols = ["biosample", "project", "urban_status", "urban_context", "country_iso2", "continent_name",
            "place_id", "place_name", "place_population", "place_lat", "place_lon", "collection_date",
            "cohort_runs", "cohort_bases"]
    cohort[cols].to_csv(os.path.join(a.outdir, "cohort_samples.tsv"), sep="\t", index=False)
    cr = r[r["biosample"].isin(cohort["biosample"])][["run", "biosample", "project", "library_layout",
                                                       "bases", "spots", "instrument_model"]]
    cr.to_csv(os.path.join(a.outdir, "cohort_runs.tsv"), sep="\t", index=False)

    feas = (elig.groupby(["urban_context", "continent_name"]).size().rename("eligible").to_frame()
            .join(cohort.groupby(["urban_context", "continent_name"]).size().rename("selected"))
            .fillna(0).astype(int).reset_index())
    feas.to_csv(os.path.join(a.outdir, "feasibility.tsv"), sep="\t", index=False)
    capped = np.minimum(cohort["cohort_bases"], a.subsample_bases).sum()
    doc = {
        "eligible_samples": int(len(elig)), "selected_samples": int(len(cohort)),
        "selected_runs": int(len(cr)), "target": a.target,
        "habitats": cohort["urban_context"].value_counts().to_dict(),
        "cities": int(cohort["place_id"].nunique()), "countries": int(cohort["country_iso2"].nunique()),
        "bioprojects": int(cohort["project"].nunique()),
        "download_tb_raw_bases": float(cohort["cohort_bases"].sum() / 1e12),
        "analysed_tb_after_cap": float(capped / 1e12),
        "criteria": {k: v for k, v in vars(a).items() if k not in ("samples", "runs", "outdir")},
    }
    with open(os.path.join(a.outdir, "feasibility.json"), "w") as fh:
        json.dump(doc, fh, indent=2, default=str)
        fh.write("\n")
    print(json.dumps({k: doc[k] for k in ("eligible_samples", "selected_samples", "cities", "countries",
                                         "bioprojects", "download_tb_raw_bases")}))


if __name__ == "__main__":
    main()
