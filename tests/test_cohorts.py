"""Offline tests for cohorts and collections. Run: python -m unittest discover tests"""
import json
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from umdb_harvester import cohorts  # noqa: E402

SCHEMA = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE projects (accession TEXT PRIMARY KEY, title TEXT, center_name TEXT);
CREATE TABLE samples (accession TEXT PRIMARY KEY, urban_status TEXT, urban_context TEXT, collection_date TEXT,
    collection_year INTEGER, collection_date_raw TEXT, lat REAL, lon REAL);
CREATE TABLE project_samples (project_accession TEXT, sample_accession TEXT);
CREATE TABLE experiments (accession TEXT PRIMARY KEY, assay_class TEXT, library_layout TEXT, platform TEXT,
    instrument_model TEXT, library_strategy TEXT);
CREATE TABLE runs (accession TEXT PRIMARY KEY, experiment_accession TEXT, sample_accession TEXT, project_accession TEXT,
    release_date TEXT, release_year INTEGER, bases INTEGER, spots INTEGER, download_url TEXT);
CREATE TABLE countries (iso2 TEXT PRIMARY KEY, name TEXT);
CREATE TABLE admin1 (code TEXT PRIMARY KEY, name TEXT);
CREATE TABLE places (geonames_id INTEGER PRIMARY KEY, name TEXT);
CREATE TABLE sample_locations (sample_accession TEXT PRIMARY KEY, country_iso2 TEXT, country_status TEXT,
    admin1_code TEXT, place_id INTEGER, status TEXT);
"""


def fixture_db(path):
    con = sqlite3.connect(path)
    con.executescript(SCHEMA)
    con.executemany("INSERT INTO meta VALUES (?,?)", [("built_utc", "2026-11-16T00:00:00+00:00"), ("rules_version", "t")])
    con.executemany("INSERT INTO projects VALUES (?,?,?)", [("PRJ1", "Subway survey", "Lab A"), ("PRJ2", "Sewage study", "Lab B")])
    con.execute("INSERT INTO countries VALUES ('US','United States')")
    con.execute("INSERT INTO places VALUES (5128581,'New York City')")
    rows = [  # sample, project, tier, context, year, assay, place?
        ("SAMN1", "PRJ1", "confirmed_urban", "transit", 2016, "WGS", True),
        ("SAMN2", "PRJ1", "confirmed_urban", "transit", 2016, "16S", True),
        ("SAMN3", "PRJ2", "probable_urban", "municipal_wastewater", 2021, "WGS", True),
        ("SAMN4", "PRJ2", "probable_urban", "municipal_wastewater", 2022, "WGS", False),
        ("SAMN5", "PRJ2", "uncertain", "municipal_wastewater", 2022, "WGS", False),
    ]
    for i, (s, p, tier, ctx, y, assay, placed) in enumerate(rows):
        con.execute("INSERT INTO samples VALUES (?,?,?,?,?,?,?,?)", (s, tier, ctx, f"{y}-06-01", y, f"{y}-06-01", 40.7, -74.0))
        con.execute("INSERT INTO project_samples VALUES (?,?)", (p, s))
        con.execute("INSERT INTO experiments VALUES (?,?,?,?,?,?)", (f"SRX{i}", assay, "PAIRED", "ILLUMINA", "NovaSeq", "WGS"))
        con.execute("INSERT INTO runs VALUES (?,?,?,?,?,?,?,?,?)", (f"SRR{i}", f"SRX{i}", s, p, f"{y + 1}-01-01", y + 1, 10 ** 9, 10 ** 6, ""))
        con.execute("INSERT INTO sample_locations VALUES (?,?,?,?,?,?)",
                    (s, "US", "verified", None, 5128581 if placed else None, "verified" if placed else "unresolved"))
    con.commit()
    return con


class PrngTests(unittest.TestCase):
    def test_matches_javascript_mulberry32(self):
        # Values printed by the browser implementation (UMA docs/assets/workspace.js) for seed 20261119;
        # UMA's tests/test_build.py checks the same four values.
        r = cohorts.mulberry32(20261119)
        self.assertEqual([f"{r():.12f}" for _ in range(4)],
                         ["0.582067719661", "0.366967471316", "0.278875814751", "0.921800950775"])

    def test_balance_caps_and_is_deterministic(self):
        samples = [{"biosample": f"S{i:03d}", "bioproject": f"P{i % 3}", "place_id": i % 2, "habitat": "transit"}
                   for i in range(60)]
        a = cohorts.balance(samples, max_per_cell=10, max_per_project=100, seed=1)
        b = cohorts.balance(list(reversed(samples)), max_per_cell=10, max_per_project=100, seed=1)
        self.assertEqual([s["biosample"] for s in a], [s["biosample"] for s in b])
        self.assertEqual(len(a), 20)
        self.assertEqual(len(cohorts.balance(samples, target=7, seed=3)), 7)
        self.assertIs(cohorts.balance(samples), samples)


class CohortTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "t.sqlite")
        self.con = fixture_db(self.db)

    def tearDown(self):
        self.con.close()
        self.tmp.cleanup()

    def ids(self, cohort):
        return [s["biosample"] for s in cohorts.evaluate(self.con, cohort)[0]]

    def test_default_filters_are_urban_tiers(self):
        self.assertEqual(self.ids({"filters": {}}), ["SAMN1", "SAMN2", "SAMN3", "SAMN4"])

    def test_filters_match_explorer_semantics(self):
        self.assertEqual(self.ids({"filters": {"context": ["transit"], "assay": ["WGS"]}}), ["SAMN1"])
        self.assertEqual(self.ids({"filters": {"loc": "place"}}), ["SAMN1", "SAMN2", "SAMN3"])
        self.assertEqual(self.ids({"filters": {"cmin": 2021, "tier": []}}), ["SAMN3", "SAMN4", "SAMN5"])
        self.assertEqual(self.ids({"filters": {"q": "sewage"}}), ["SAMN3", "SAMN4"])
        self.assertEqual(self.ids({"filters": {"ymax": 2017}}), ["SAMN1", "SAMN2"])
        self.assertEqual(self.ids({"filters": {"city": "5128581", "project": "PRJ2"}}), ["SAMN3"])

    def test_set_operations(self):
        a = {"filters": {"tier": []}}
        b = {"filters": {"context": ["transit"]}}
        self.assertEqual(self.ids({"op": "difference", "a": a, "b": b}), ["SAMN3", "SAMN4", "SAMN5"])
        self.assertEqual(self.ids({"op": "intersection", "a": a, "b": b}), ["SAMN1", "SAMN2"])
        self.assertEqual(len(self.ids({"op": "union", "a": b, "b": {"filters": {"q": "sewage"}}})), 4)

    def test_assay_filter_keeps_only_those_runs(self):
        samples, runs = cohorts.evaluate(self.con, {"filters": {"assay": ["16S"]}})
        self.assertEqual([r["assay_class"] for r in runs], ["16S"])
        self.assertEqual(samples[0]["assays"], "16S")

    def test_package_files_and_cli_roundtrip(self):
        out = os.path.join(self.tmp.name, "pkg")
        counts = cohorts.export_cohort({"name": "Test", "filters": {"context": ["municipal_wastewater"]}}, out, self.db)
        self.assertEqual(counts["biosamples"], 2)
        for name in ("README.md", "cohort.json", "metadata.tsv", "runs.tsv", "accessions.txt", "download_fastq.sh",
                     "samplesheet.csv", "cohort_summary.json", "analysis.py", "analysis.R", "CITATION.bib"):
            self.assertTrue(os.path.exists(os.path.join(out, name)), name)
        manifest = json.load(open(os.path.join(out, "cohort.json")))
        self.assertEqual(manifest["release"]["database_built_utc"], "2026-11-16T00:00:00+00:00")
        # cohort.json rebuilds the same cohort
        again = cohorts.evaluate(self.con, manifest["definition"])[0]
        self.assertEqual([s["biosample"] for s in again], ["SAMN3", "SAMN4"])
        sheet = open(os.path.join(out, "samplesheet.csv")).read().splitlines()
        self.assertEqual(sheet[0], "sample,run_accession,instrument_platform,fastq_1,fastq_2,fasta")
        self.assertIn("fastq/SRR2_2.fastq.gz", sheet[1])
        self.assertTrue(open(os.path.join(out, "runs.tsv")).readline().startswith("run\tbiosample\tproject"))

    def test_collections_are_defined(self):
        slugs = [c["slug"] for c in cohorts.COLLECTIONS]
        self.assertEqual(len(slugs), len(set(slugs)))
        self.assertIn("global-wastewater", slugs)
        with self.assertRaises(KeyError):
            cohorts.collection_cohort("nope")


if __name__ == "__main__":
    unittest.main()
