#!/usr/bin/env python3
"""C0 · Data-quality audit: what must be fixed or excluded before inference.

Counts, by eligibility tier, assay and continent: samples without a resolved place,
without coordinates or a collection date, with collection dates in the future or
after the data were released, and with coordinates that contradict the stated place.
Downstream tests use the snapshot's ``date_ok`` and ``has_place`` flags to exclude
these samples; this report says how many that removes and where.
"""
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from umdb_common import base_parser, parse_args, read_tsv, write_summary, write_table  # noqa: E402

TITLE = "Data-quality audit"
QUESTION = "How much of the catalog is usable for spatial and temporal inference, and where are the gaps?"
CHECKS = ["has_place", "has_coordinates", "has_collection_date", "flag_date_future",
          "flag_date_after_release", "flag_date_implausible", "flag_coords_conflict"]


def table(df, by):
    t = df.groupby(by, dropna=False)[CHECKS].mean().mul(100).round(2)
    t.insert(0, "samples", df.groupby(by, dropna=False).size())
    return t.reset_index()


def main():
    a = parse_args(base_parser(__doc__))
    s = read_tsv(a.samples)
    s["continent"] = s["continent"].fillna("unresolved")
    files = [write_table(table(s, "urban_status"), a.outdir, "audit_by_tier.tsv"),
             write_table(table(s, "primary_assay"), a.outdir, "audit_by_assay.tsv")]
    u = s[s["urban_status"].isin(["confirmed_urban", "probable_urban"])]
    files.append(write_table(table(u, "continent"), a.outdir, "audit_urban_by_continent.tsv"))
    bad = s[s["flag_date_future"] | s["flag_date_after_release"] | s["flag_date_implausible"]]
    files.append(write_table(bad[["biosample", "project", "urban_status", "collection_date_raw",
                                  "collection_year", "first_release_date", "flag_date_future",
                                  "flag_date_after_release", "flag_date_implausible"]],
                             a.outdir, "date_anomalies.tsv"))
    res = {
        "samples": len(s), "urban_samples": len(u),
        "pct_without_place_all": round(100 * (1 - s["has_place"].mean()), 2),
        "pct_without_place_urban": round(100 * (1 - u["has_place"].mean()), 2),
        "pct_without_collection_date_urban": round(100 * (1 - u["has_collection_date"].mean()), 2),
        "date_future": int(s["flag_date_future"].sum()),
        "date_after_release": int(s["flag_date_after_release"].sum()),
        "date_implausible": int(s["flag_date_implausible"].sum()),
        "coords_conflict": int(s["flag_coords_conflict"].sum()),
        "urban_usable_spatial": int(u["has_place"].sum()),
        "urban_usable_spatiotemporal": int((u["has_place"] & u["has_collection_date"] & u["date_ok"]).sum()),
    }
    write_summary(a.outdir, a.id, TITLE, QUESTION, "descriptive", res,
                  interpretation=(f"{res['pct_without_place_urban']}% of urban BioSamples lack a resolved place; "
                                  f"{res['urban_usable_spatiotemporal']:,} have a place and a plausible full date. "
                                  f"{res['date_future'] + res['date_after_release'] + res['date_implausible']:,} "
                                  "samples have impossible collection dates and are excluded from temporal tests."),
                  caveats=["Places under 500 inhabitants, districts and facility names are not resolved by design.",
                           "A collection date after the first run release is impossible and is treated as an error."],
                  files=files)


if __name__ == "__main__":
    main()
