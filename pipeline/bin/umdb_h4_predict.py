#!/usr/bin/env python3
"""H4 · Can environment, climate and demography predict communities in unsampled cities?

Features per sample: habitat, latitude (signed and absolute), longitude (sin/cos),
climate band, log city population, log GDP per capita, collection month (sin/cos) and
year. Target: Hellinger-transformed relative abundance of the --top most abundant
species. Model: random forest (multi-output).

Skill = 1 - BC(predicted, observed) / BC(training-mean profile, observed), so 0 is no
better than predicting the average community. Three cross-validation schemes:
  random 5-fold (optimistic), leave-cities-out (GroupKFold by city: the critical test),
  leave-studies-out (GroupKFold by BioProject).
The gap between random and leave-cities-out skill is the part of apparent
predictability that is really memorisation of places.

Decision rule: "supported" when leave-cities-out skill > 0 in every fold.
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import (base_parser, climate_band, load_ecology, parse_args, plt, skip,  # noqa: E402
                         stratified_subsample, write_summary, write_table)

TITLE = "Predicting communities in unsampled cities"
QUESTION = "Do environmental, climatic and demographic features predict community composition in held-out cities?"


def features(meta):
    f = pd.DataFrame(index=meta.index)
    f["lat"] = meta["place_lat"].astype(float)
    f["abs_lat"] = f["lat"].abs()
    lon = np.radians(meta["place_lon"].astype(float))
    f["lon_sin"], f["lon_cos"] = np.sin(lon), np.cos(lon)
    f["log_pop"] = np.log10(meta["place_population"].astype(float).clip(lower=1))
    if "gdp_per_capita_usd2015" in meta:
        f["log_gdp"] = np.log10(meta["gdp_per_capita_usd2015"].astype(float))
    d = pd.to_datetime(meta["collection_date"], errors="coerce")
    f["month_sin"] = np.sin(2 * np.pi * d.dt.month / 12)
    f["month_cos"] = np.cos(2 * np.pi * d.dt.month / 12)
    f["year"] = d.dt.year
    cat = pd.get_dummies(pd.DataFrame({"habitat": meta["urban_context"],
                                       "climate": meta["place_lat"].astype(float).map(climate_band)}),
                         dtype=float)
    f = pd.concat([f, cat], axis=1)
    return f.fillna(f.median(numeric_only=True))


def bc_rows(P, Y):
    num = np.abs(P - Y).sum(axis=1)
    den = (P + Y).sum(axis=1)
    return num / np.where(den == 0, 1, den)


def main():
    p = base_parser(__doc__)
    p.add_argument("--top", type=int, default=50)
    p.add_argument("--trees", type=int, default=300)
    a = parse_args(p)
    X, meta = load_ecology(a, min_samples=40)
    if X is None:
        skip(a.outdir, a.id, TITLE, QUESTION, "No species profiles (or fewer than 40 profiled urban samples).")
    meta = stratified_subsample(meta, ["urban_context", "place_id"], max(a.max_samples * 3, 5000), a.seed)
    if meta["place_id"].nunique() < 5:
        skip(a.outdir, a.id, TITLE, QUESTION, "Fewer than 5 cities; leave-cities-out is not meaningful.")
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.model_selection import GroupKFold, KFold

    R = X.loc[meta.index]
    R = R.div(R.sum(axis=1).replace(0, 1), axis=0)
    top = R.mean().nlargest(a.top).index
    Y = np.sqrt(R[top].to_numpy())
    F = features(meta).to_numpy()
    schemes = {"random": KFold(5, shuffle=True, random_state=a.seed).split(F),
               "leave_cities_out": GroupKFold(5).split(F, groups=meta["place_id"]),
               "leave_studies_out": GroupKFold(min(5, meta["project"].nunique())).split(F, groups=meta["project"])}
    rows = []
    for name, splits in schemes.items():
        for k, (tr, te) in enumerate(splits):
            rf = RandomForestRegressor(n_estimators=a.trees, min_samples_leaf=5, n_jobs=-1, random_state=a.seed)
            rf.fit(F[tr], Y[tr])
            P = rf.predict(F[te]) ** 2
            O = Y[te] ** 2
            N = np.tile(Y[tr].mean(axis=0) ** 2, (len(te), 1))
            skill = 1 - bc_rows(P, O).mean() / bc_rows(N, O).mean()
            rows.append({"scheme": name, "fold": k, "n_test": len(te), "bc_model": bc_rows(P, O).mean(),
                         "bc_null": bc_rows(N, O).mean(), "skill": skill})
    cv = pd.DataFrame(rows)
    summ = cv.groupby("scheme")["skill"].agg(["mean", "min", "max"]).to_dict(orient="index")
    rf = RandomForestRegressor(n_estimators=a.trees, min_samples_leaf=5, n_jobs=-1, random_state=a.seed).fit(F, Y)
    imp = pd.Series(rf.feature_importances_, index=features(meta).columns).sort_values(ascending=False)
    files = [write_table(cv, a.outdir, "cv_skill.tsv"),
             write_table(imp.rename("importance").reset_index().rename(columns={"index": "feature"}), a.outdir,
                         "feature_importance.tsv")]
    pyplot = plt()
    if pyplot:
        fig, ax = pyplot.subplots(figsize=(4.6, 3.2))
        order = ["random", "leave_studies_out", "leave_cities_out"]
        ax.boxplot([cv.loc[cv["scheme"] == s, "skill"] for s in order])
        ax.set_xticks([1, 2, 3], ["random", "held-out\nstudies", "held-out\ncities"])
        ax.axhline(0, ls="--", lw=0.8, color="#666")
        ax.set(ylabel="Skill vs mean community")
        fig.tight_layout()
        fig.savefig(os.path.join(a.outdir, "prediction.png"))
        files.append("prediction.png")
    city = cv[cv["scheme"] == "leave_cities_out"]["skill"]
    status = "supported" if (city > 0).all() else "not_supported"
    write_summary(a.outdir, a.id, TITLE, QUESTION, status,
                  {"samples": len(meta), "cities": meta["place_id"].nunique(), "target_species": len(top),
                   "skill": summ, "optimism_gap_random_minus_city": summ["random"]["mean"] -
                   summ["leave_cities_out"]["mean"], "top_features": imp.head(8).to_dict()},
                  interpretation=(f"Held-out-city skill {summ['leave_cities_out']['mean']:.3f} "
                                  f"(random CV {summ['random']['mean']:.3f}); top feature: {imp.index[0]}."),
                  caveats=["A baseline for the planned foundation model, not the model itself.",
                           "Climate is a latitude-band proxy unless a Köppen raster is supplied."],
                  files=files)


if __name__ == "__main__":
    main()
