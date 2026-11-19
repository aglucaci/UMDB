#!/usr/bin/env python3
"""C7 · Do sequencing technologies diffuse unevenly across countries?

For each technology (long-read: Oxford Nanopore or PacBio; Illumina NovaSeq; DNBSEQ/BGI),
finds the first release year in which each country with >= --min-samples urban samples
contributed one, and the global introduction year (first year the technology is >= 1% of
urban samples). Adoption lag = country first year - global year. Tests whether lag
falls with GDP per capita (Spearman) and reports long-read share by year and continent.

Countries that never adopted within the observation window are right-censored; they are
included in a Kaplan–Meier-style summary (share adopted by lag) and excluded from the
correlation, which therefore understates inequality.

Decision rule: "supported" when lag is negatively associated with GDP (p < 0.05) for at
least one technology.
"""
import os
import sys

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import CONTINENTS, base_parser, load_samples, parse_args, plt, read_tsv, write_summary, write_table  # noqa: E402

TITLE = "Diffusion of sequencing technologies"
QUESTION = "Do new sequencing technologies reach countries at different times, and does lag track wealth?"


def tech(row):
    out = set()
    pl = str(row["platforms"])
    ins = str(row["primary_instrument"])
    if "OXFORD_NANOPORE" in pl or "PACBIO" in pl:
        out.add("long_read")
    if "NovaSeq" in ins:
        out.add("illumina_novaseq")
    if "DNBSEQ" in pl or "BGISEQ" in pl:
        out.add("dnbseq_bgi")
    return out


def main():
    p = base_parser(__doc__)
    p.add_argument("--min-samples", type=int, default=50)
    a = parse_args(p)
    s = load_samples(a.samples)
    s = s[s["country_status"].isin(["verified", "probable"])].dropna(subset=["first_release_year"])
    s["first_release_year"] = s["first_release_year"].astype(int)
    s["tech"] = s.apply(tech, axis=1)
    n_country = s.groupby("country_iso2").size()
    countries = n_country[n_country >= a.min_samples].index
    s = s[s["country_iso2"].isin(countries)]
    cov = read_tsv(a.country_covariates)
    gdp = cov.set_index("iso2").get("gdp_per_capita_usd2015") if cov is not None else None
    last_year = int(s["first_release_year"].max())

    rows, results = [], {}
    for t in ("long_read", "illumina_novaseq", "dnbseq_bgi"):
        has = s["tech"].map(lambda x: t in x)
        share = has.groupby(s["first_release_year"]).mean()
        intro = share[share >= 0.01]
        if intro.empty:
            continue
        y0 = int(intro.index.min())
        first = s[has].groupby("country_iso2")["first_release_year"].min()
        for cc in countries:
            fy = first.get(cc)
            rows.append({"technology": t, "country_iso2": cc, "global_intro_year": y0,
                         "first_year": None if fy is None or pd.isna(fy) else int(fy),
                         "lag_years": None if fy is None or pd.isna(fy) else max(0, int(fy) - y0),
                         "adopted": fy is not None and not pd.isna(fy),
                         "gdp_pc": None if gdp is None else gdp.get(cc)})
        d = pd.DataFrame([r for r in rows if r["technology"] == t])
        adopted = d[d["adopted"]].dropna(subset=["gdp_pc"])
        rho = spearmanr(adopted["lag_years"], np.log(adopted["gdp_pc"].astype(float))) \
            if len(adopted) > 5 else None
        results[t] = {"global_intro_year": y0, "countries": len(d), "adopted": int(d["adopted"].sum()),
                      "censored": int((~d["adopted"]).sum()), "median_lag": float(d["lag_years"].dropna().median()),
                      "lag_vs_log_gdp_spearman": None if rho is None else {"rho": rho.statistic, "p": rho.pvalue},
                      "share_adopted_within_3y": float((d["lag_years"].dropna() <= 3).sum() / len(d))}
    lag = pd.DataFrame(rows)
    s["continent_name"] = s["continent"].map(lambda k: CONTINENTS.get(k, k))
    lr = s.assign(long_read=s["tech"].map(lambda x: "long_read" in x)) \
        .pivot_table(index="first_release_year", columns="continent_name", values="long_read", aggfunc="mean")
    files = [write_table(lag, a.outdir, "adoption_lag.tsv"),
             write_table(lr.round(4).reset_index(), a.outdir, "long_read_share_by_year_continent.tsv")]
    pyplot = plt()
    if pyplot and len(lag):
        fig, ax = pyplot.subplots(figsize=(5, 3.2))
        for t, g in lag.groupby("technology"):
            lags = np.sort(g["lag_years"].dropna().to_numpy())
            frac = np.arange(1, len(lags) + 1) / len(g)
            ax.step(lags, frac, where="post", label=t.replace("_", " "))
        ax.set(xlabel="Years after global introduction", ylabel="Share of countries adopted", ylim=(0, 1))
        ax.legend(frameon=False, fontsize=7)
        fig.tight_layout()
        fig.savefig(os.path.join(a.outdir, "adoption.png"))
        files.append("adoption.png")
    ok = any(v["lag_vs_log_gdp_spearman"] and v["lag_vs_log_gdp_spearman"]["rho"] < 0
             and v["lag_vs_log_gdp_spearman"]["p"] < 0.05 for v in results.values())
    write_summary(a.outdir, a.id, TITLE, QUESTION, "supported" if ok else "not_supported",
                  {"technologies": results, "countries": len(countries), "observation_end": last_year},
                  interpretation="; ".join(f"{t}: {v['adopted']}/{v['countries']} countries adopted, median lag "
                                           f"{v['median_lag']:.0f} y" for t, v in results.items()),
                  caveats=["Adoption is observed only through public urban data; a country may use a platform "
                           "for clinical or unpublished work earlier.",
                           "Release year, not sequencing year, is used (sequencing dates are not reported)."],
                  files=files)


if __name__ == "__main__":
    main()
