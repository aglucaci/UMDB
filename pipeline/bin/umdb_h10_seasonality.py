#!/usr/bin/env python3
"""H10 · Is community turnover seasonal, and does it follow local (hemisphere-aligned) seasons?

Samples with a plausible full collection date. Responses: PCoA axes of
Bray–Curtis dissimilarity (first five), ordinated separately within each habitat, and log species
richness. Each response is demeaned within study x habitat (a fixed effect per time
series; only studies spanning >= --min-months distinct months), then compared across
models by AIC:
  none        no seasonal term
  calendar    sin/cos of day of year
  aligned     sin/cos of day of year shifted by half a year in the southern hemisphere
  aligned×climate  aligned harmonics interacting with climate band

Decision rule: "supported" when, for composition (AIC summed over PCo1–5; seasonal
taxa need not load on the first axis), the aligned model beats both "none" and
"calendar" by ΔAIC > 2. Climate-structured seasonality further predicts that the
interaction model improves on "aligned" (tropical cities with weak seasons).
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import (base_parser, bray_curtis, climate_band, load_ecology, parse_args, pcoa, plt, skip,  # noqa: E402
                         stratified_subsample, write_summary, write_table)

NAX = 5  # ordination axes per habitat

TITLE = "Seasonality across climate zones"
QUESTION = "Do communities turn over seasonally, following the local rather than the calendar season?"


def aic_ols(y, X):
    X = np.column_stack([np.ones(len(y)), X]) if X.size else np.ones((len(y), 1))
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    rss = float(np.sum((y - X @ beta) ** 2))
    n, k = len(y), X.shape[1]
    return n * np.log(rss / n) + 2 * k, beta


def main():
    p = base_parser(__doc__)
    p.add_argument("--min-months", type=int, default=4)
    a = parse_args(p)
    X, meta = load_ecology(a)
    if X is None:
        skip(a.outdir, a.id, TITLE, QUESTION, "No species profiles.")
    meta = meta[meta["date_ok"].astype(bool) & meta["collection_date"].notna()].copy()
    meta["date"] = pd.to_datetime(meta["collection_date"], errors="coerce")
    meta = meta.dropna(subset=["date"])
    span = meta.groupby("project")["date"].apply(lambda d: d.dt.month.nunique())
    meta = meta[meta["project"].isin(span[span >= a.min_months].index)]
    if len(meta) < 30:
        skip(a.outdir, a.id, TITLE, QUESTION, "Fewer than 30 dated samples in multi-month studies.")
    meta = stratified_subsample(meta, ["urban_context", "place_id"], a.max_samples, a.seed)
    R = X.loc[meta.index].to_numpy()
    # ordinate each habitat separately so PCo axes describe within-habitat turnover, not habitat identity
    pc = np.full((len(meta), NAX), np.nan)
    hab = meta["urban_context"].to_numpy()
    for h in np.unique(hab):
        ix = np.where(hab == h)[0]
        if ix.size >= 15:
            pc[ix] = pcoa(bray_curtis(R[ix]), NAX)
    ok_rows = ~np.isnan(pc[:, 0])
    meta, R, pc = meta[ok_rows], R[ok_rows], pc[ok_rows]
    doy = meta["date"].dt.day_of_year.to_numpy(float)
    south = meta["place_lat"].astype(float).to_numpy() < 0
    aligned = doy + np.where(south, 182.625, 0)
    w = 2 * np.pi / 365.25
    cal = np.column_stack([np.sin(w * doy), np.cos(w * doy)])
    ali = np.column_stack([np.sin(w * aligned), np.cos(w * aligned)])
    band = pd.get_dummies(meta["place_lat"].astype(float).map(climate_band), drop_first=True, dtype=float).to_numpy()
    inter = np.column_stack([ali] + [ali * band[:, [j]] for j in range(band.shape[1])]) if band.size else ali
    responses = {f"PCo{k + 1}": pc[:, k] for k in range(NAX)}
    responses["log_richness"] = np.log((R > 0).sum(axis=1).clip(min=1))
    proj = (meta["project"] + "|" + meta["urban_context"]).to_numpy()  # study x habitat time series
    rows, amp = [], {}
    for name, y in responses.items():
        yd = y - pd.Series(y).groupby(proj).transform("mean").to_numpy()

        def demean(M):
            return M - pd.DataFrame(M).groupby(proj).transform("mean").to_numpy()

        res = {}
        for m, M in {"none": np.empty((len(y), 0)), "calendar": cal, "aligned": ali,
                     "aligned_x_climate": inter}.items():
            res[m], beta = aic_ols(yd, demean(M) if M.size else M)
            if m == "aligned":
                amp[name] = float(np.hypot(beta[1], beta[2]))
        rows.append({"response": name, **{f"aic_{k}": v for k, v in res.items()},
                     "dAIC_aligned_vs_none": res["none"] - res["aligned"],
                     "dAIC_aligned_vs_calendar": res["calendar"] - res["aligned"],
                     "dAIC_interaction_vs_aligned": res["aligned"] - res["aligned_x_climate"]})
    tab = pd.DataFrame(rows)
    # composition as a whole: AIC summed over the first NAX axes (season need not load on PCo1)
    multi = tab[tab["response"].str.startswith("PCo")].drop(columns="response").sum()
    tab = pd.concat([tab, pd.DataFrame([{"response": f"composition_PCo1-{NAX}", **multi.to_dict()}])],
                    ignore_index=True)
    files = [write_table(tab, a.outdir, "model_comparison.tsv")]
    pyplot = plt()
    if pyplot:
        fig, ax = pyplot.subplots(figsize=(5, 3.2))
        y = responses["PCo1"] - pd.Series(responses["PCo1"]).groupby(proj).transform("mean").to_numpy()
        ax.scatter(aligned % 365.25, y, s=5, alpha=0.5, c=np.where(south, "#c44536", "#2a6f97"))
        ax.set(xlabel="Hemisphere-aligned day of year", ylabel="PCo1 (within habitat and study)",
               title="red: southern hemisphere")
        fig.tight_layout()
        fig.savefig(os.path.join(a.outdir, "seasonality.png"))
        files.append("seasonality.png")
    r = tab.set_index("response").loc[f"composition_PCo1-{NAX}"]
    ok = r["dAIC_aligned_vs_none"] > 2 and r["dAIC_aligned_vs_calendar"] > 2
    write_summary(a.outdir, a.id, TITLE, QUESTION, "supported" if ok else "not_supported",
                  {"samples": len(meta), "studies": int(meta["project"].nunique()),
                   "southern_hemisphere_samples": int(south.sum()),
                   "models": tab.set_index("response").to_dict(orient="index"), "aligned_amplitude": amp},
                  interpretation=(f"Composition (PCo1–{NAX}): aligned seasonal model beats no-season by ΔAIC {r['dAIC_aligned_vs_none']:.1f} "
                                  f"and calendar season by ΔAIC {r['dAIC_aligned_vs_calendar']:.1f}."),
                  caveats=["The hemisphere test needs southern-hemisphere time series; without them aligned and "
                           "calendar models are identical (ΔAIC 0).",
                           "Climate is a latitude-band proxy unless a Köppen raster is supplied; ERA5 temperature "
                           "and precipitation are planned covariates."],
                  files=files)


if __name__ == "__main__":
    main()
