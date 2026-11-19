#!/usr/bin/env python3
"""C3 · Did COVID-19 structurally change urban sequencing? Interrupted time series.

Monthly counts of urban BioSamples by collection month (plausible full dates only),
from --start to --end, modelled as segmented regression on log(count + 1):
    y_t = b0 + b1 t + b2 post_t + b3 (t - t0) post_t + month-of-year + e_t
with the interruption t0 = 2020-03 and Newey–West (HAC, 12-month) standard errors.
b2 is the immediate level change and b3 the change in trend. Fitted separately for all
urban samples, municipal wastewater and every other habitat, and for new BioProjects
by first release month. A composition test compares the wastewater share before and after.

The series ends at --end (default 2024-12) because recent collections are still being
released (right truncation would look like a post-pandemic decline). Sensitivity fits
(a lagged 2021-01 break; an end at 2023-06) are reported but do not enter the decision.

Decision rule: "supported" when the level or slope change for all urban samples has
p < 0.05 and the wastewater share differs before vs after (chi-square p < 0.05).
"""
import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import base_parser, load_samples, parse_args, plt, write_summary, write_table  # noqa: E402

TITLE = "COVID-19 and the structure of urban sequencing"
QUESTION = "Did the pandemic cause a level or trend break in urban sequencing, and shift it toward wastewater?"


def its(series: pd.Series, t0: pd.Period):
    import statsmodels.api as sm
    df = pd.DataFrame({"y": np.log1p(series.values)}, index=series.index)
    df["t"] = np.arange(len(df))
    df["post"] = (df.index >= t0).astype(float)
    k0 = int(np.argmax(df.index >= t0))
    df["t_post"] = np.where(df["post"] == 1, df["t"] - k0, 0.0)
    month = pd.get_dummies(df.index.month, prefix="m", drop_first=True, dtype=float)
    month.index = df.index
    X = sm.add_constant(pd.concat([df[["t", "post", "t_post"]], month], axis=1))
    r = sm.OLS(df["y"], X).fit(cov_type="HAC", cov_kwds={"maxlags": 12})
    ci = r.conf_int()
    out = {}
    for term in ("post", "t_post", "t"):
        out[term] = {"coef": r.params[term], "ci_low": ci.loc[term, 0], "ci_high": ci.loc[term, 1],
                     "p": r.pvalues[term]}
    out["level_change_pct"] = 100 * (np.exp(r.params["post"]) - 1)
    out["annual_trend_change_pct"] = 100 * (np.exp(12 * r.params["t_post"]) - 1)
    return out, r.fittedvalues


def main():
    p = base_parser(__doc__)
    p.add_argument("--start", default="2015-01")
    p.add_argument("--end", default="2024-12")
    p.add_argument("--break", dest="brk", default="2020-03")
    a = parse_args(p)
    s = load_samples(a.samples)
    s = s[s["has_collection_date"] & s["date_ok"]].copy()
    s["month"] = pd.PeriodIndex(pd.to_datetime(s["collection_date"], errors="coerce"), freq="M")
    s = s.dropna(subset=["month"])
    idx = pd.period_range(a.start, a.end, freq="M")
    t0 = pd.Period(a.brk, freq="M")
    s = s[(s["month"] >= idx[0]) & (s["month"] <= idx[-1])]
    ww = s["urban_context"].eq("municipal_wastewater")

    series = {
        "all_urban_samples": s.groupby("month").size(),
        "wastewater_samples": s[ww].groupby("month").size(),
        "other_habitat_samples": s[~ww].groupby("month").size(),
    }
    runs = pd.read_csv(a.runs, sep="\t", usecols=["project", "release_date"], low_memory=False) if a.runs else None
    if runs is not None:
        rel = runs.dropna().groupby("project")["release_date"].min()
        rel = rel[rel.index.isin(s["project"].unique())]
        pm = pd.PeriodIndex(pd.to_datetime(rel, errors="coerce"), freq="M")
        series["new_urban_bioprojects_by_release"] = pd.Series(1, index=pm).groupby(level=0).sum()
    series = {k: v.reindex(idx, fill_value=0) for k, v in series.items()}

    results, fitted = {}, {}
    for k, v in series.items():
        results[k], fitted[k] = its(v, t0)
    monthly = pd.DataFrame(series)
    monthly.index = monthly.index.astype(str)

    pre, post = s[s["month"] < t0], s[s["month"] >= t0]
    from scipy.stats import chi2_contingency
    tab = np.array([[pre["urban_context"].eq("municipal_wastewater").sum(),
                     (~pre["urban_context"].eq("municipal_wastewater")).sum()],
                    [post["urban_context"].eq("municipal_wastewater").sum(),
                     (~post["urban_context"].eq("municipal_wastewater")).sum()]])
    chi = chi2_contingency(tab)
    comp = {"wastewater_share_pre": tab[0, 0] / tab[0].sum(), "wastewater_share_post": tab[1, 0] / tab[1].sum(),
            "chi2": chi[0], "p": chi[1]}
    habitat_shift = (pd.crosstab(s["month"] >= t0, s["urban_context"], normalize="index").T
                     .rename(columns={False: "share_pre", True: "share_post"}))

    # Sensitivity (reported, never used for the decision): a lagged break when surveillance
    # programmes scaled up, and an earlier end to reduce release-lag truncation.
    sens = {}
    allu = series["all_urban_samples"]
    for label, brk, end in (("break_2021-01", "2021-01", a.end), ("end_2023-06", a.brk, "2023-06")):
        sub = allu[allu.index <= pd.Period(end, freq="M")]
        r_, _ = its(sub, pd.Period(brk, freq="M"))
        sens[label] = {"level_change_pct": r_["level_change_pct"], "p_level": r_["post"]["p"],
                       "annual_trend_change_pct": r_["annual_trend_change_pct"], "p_trend": r_["t_post"]["p"]}

    rows = [{"series": k, "term": t, **v} for k, d in results.items() for t, v in d.items() if isinstance(v, dict)]
    files = [write_table(pd.DataFrame(rows), a.outdir, "its_coefficients.tsv"),
             write_table(monthly.reset_index().rename(columns={"index": "month"}), a.outdir, "monthly_counts.tsv"),
             write_table(habitat_shift.reset_index(), a.outdir, "habitat_share_pre_post.tsv")]
    pyplot = plt()
    if pyplot:
        fig, axes = pyplot.subplots(len(series), 1, figsize=(7, 1.9 * len(series)), sharex=True)
        x = idx.to_timestamp()
        for ax, (k, v) in zip(np.atleast_1d(axes), series.items()):
            ax.plot(x, np.log1p(v.values), lw=0.8, color="#7a7a7a")
            ax.plot(x, fitted[k].values, lw=1.4, color="#2a6f97")
            ax.axvline(t0.to_timestamp(), color="#c44536", ls="--", lw=0.8)
            ax.set_ylabel("log(n+1)")
            ax.set_title(k.replace("_", " "), fontsize=8, loc="left")
        fig.tight_layout()
        fig.savefig(os.path.join(a.outdir, "its.png"))
        files.append("its.png")

    r = results["all_urban_samples"]
    ok = (r["post"]["p"] < 0.05 or r["t_post"]["p"] < 0.05) and comp["p"] < 0.05
    write_summary(a.outdir, a.id, TITLE, QUESTION, "supported" if ok else "not_supported",
                  {"series": results, "composition": comp, "sensitivity_all_urban": sens, "samples_in_window": len(s),
                   "window": [a.start, a.end], "interruption": a.brk},
                  interpretation=(f"Level change at {a.brk}: {r['level_change_pct']:.0f}% (p={r['post']['p']:.2g}); "
                                  f"trend change {r['annual_trend_change_pct']:.0f}%/yr (p={r['t_post']['p']:.2g}). "
                                  f"Wastewater share {100 * comp['wastewater_share_pre']:.0f}% → "
                                  f"{100 * comp['wastewater_share_post']:.0f}%."),
                  caveats=["Discovery queries exclude SARS-CoV-2 amplicon surveillance by default, so the "
                           "wastewater surge is under-counted, not over-counted.",
                           "Collection dates are submitter-reported; 21% of urban samples have none.",
                           "Recent years are right-truncated by release lag; --end limits this."],
                  files=files)


if __name__ == "__main__":
    main()
