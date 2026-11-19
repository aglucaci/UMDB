#!/usr/bin/env python3
"""Collect every hypothesis summary.json into report.md, report.json and hypotheses_status.tsv.

Hypotheses in the registry that did not run (missing inputs, roadmap) are listed with
the reason, so the report always shows the whole research programme.
"""
import argparse
import csv
import glob
import json
import os

ICON = {"supported": "supported", "not_supported": "not supported", "inconclusive": "inconclusive",
        "descriptive": "descriptive", "skipped": "skipped", "planned": "planned", "not_run": "not run"}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--registry", required=True)
    ap.add_argument("--snapshot", required=True)
    ap.add_argument("--cohort", help="feasibility.json from umdb_select_cohort.py")
    ap.add_argument("--summaries", nargs="*", default=[])
    ap.add_argument("--outdir", default=".")
    a = ap.parse_args()
    os.makedirs(a.outdir, exist_ok=True)
    found = {}
    for path in a.summaries:
        for f in ([path] if path.endswith(".json") else glob.glob(os.path.join(path, "**", "summary.json"),
                                                                    recursive=True)):
            d = json.load(open(f))
            found[d["id"]] = d
    snap = json.load(open(a.snapshot))
    cohort = json.load(open(a.cohort)) if a.cohort and os.path.exists(a.cohort) else None
    reg = list(csv.DictReader(open(a.registry)))
    rows = []
    for r in reg:
        d = found.get(r["id"])
        if d:
            status, text = d["status"], d.get("interpretation", "")
        elif r["track"] == "roadmap":
            status, text = "planned", "Not implemented yet; see pipeline/README.md (roadmap)."
        else:
            status, text = "not_run", f"Not run: needs {r['requires'] or 'its inputs'}."
        rows.append({"id": r["id"], "track": r["track"], "program": r["program"], "title": r["title"],
                     "status": status, "result": text})
    with open(os.path.join(a.outdir, "hypotheses_status.tsv"), "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]), delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    m = snap.get("meta", {})
    lines = ["# UMDB-SCIENCE report", "",
             f"Catalog: `{snap.get('database')}` built {m.get('built_utc', '?')} · schema {m.get('schema_version', '?')}"
             f" · eligibility rules {m.get('rules_version', '?')} · query profiles {m.get('query_profiles_version', '?')}",
             f"Snapshot: {snap['rows']['samples']:,} BioSamples, {snap['rows']['runs']:,} runs"
             + (f" · SHA-256 `{snap['database_sha256'][:16]}…`" if snap.get("database_sha256") else ""), ""]
    if cohort:
        lines += [f"Shotgun cohort: {cohort['selected_samples']:,} of {cohort['eligible_samples']:,} eligible "
                  f"BioSamples, {cohort['cities']} cities, {cohort['countries']} countries, "
                  f"{cohort['bioprojects']} BioProjects; {cohort['analysed_tb_after_cap']:.1f} Tbp to analyse after "
                  "depth capping.", ""]
    lines += ["| ID | Programme | Hypothesis | Status | Result |", "| --- | --- | --- | --- | --- |"]
    for r in rows:
        lines.append(f"| {r['id']} | {r['program']} | {r['title']} | {ICON.get(r['status'], r['status'])} | "
                     f"{r['result'].replace('|', '/')} |")
    lines.append("")
    for r in rows:
        d = found.get(r["id"])
        if not d or d["status"] == "skipped":
            continue
        lines += [f"## {r['id']} · {d['title']}", "", f"**Question.** {d['question']}", "",
                  f"**Status:** {ICON.get(d['status'], d['status'])}. {d['interpretation']}", ""]
        if d.get("caveats"):
            lines += ["**Caveats.**", ""] + [f"- {c}" for c in d["caveats"]] + [""]
        if d.get("files"):
            lines += ["Files: " + ", ".join(f"`{r['id']}/{f}`" for f in d["files"]), ""]
    open(os.path.join(a.outdir, "report.md"), "w").write("\n".join(lines))
    json.dump({"snapshot": snap, "cohort": cohort, "hypotheses": rows,
               "summaries": found}, open(os.path.join(a.outdir, "report.json"), "w"), indent=2)
    print("\n".join(lines[:40]))


if __name__ == "__main__":
    main()
