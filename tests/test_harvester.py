"""Offline unit tests for the UMDB harvester. Run: python -m unittest discover tests"""
import json
import os
import sys
import importlib.util
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

from umdb_harvester import ncbi  # noqa: E402
from umdb_harvester.assay import classify_assay  # noqa: E402
from umdb_harvester.biosample import infer_geo, parse_geo_loc_name  # noqa: E402
from umdb_harvester.config import QUERY_PROFILES  # noqa: E402
from umdb_harvester.eligibility import assess_eligibility  # noqa: E402


def row(strategy="", source="", selection="", name=""):
    return {"LibraryStrategy": strategy, "LibrarySource": source, "LibrarySelection": selection, "LibraryName": name}


class AssayTests(unittest.TestCase):
    def cls(self, *a, title="", attrs=None):
        return classify_assay(row(*a), title, {"attributes": attrs or {}})["assay_class"]

    def test_metatranscriptomics_is_not_rna_seq(self):
        self.assertEqual(self.cls("RNA-Seq", "METATRANSCRIPTOMIC", "RANDOM"), "Metatranscriptomics")
        self.assertEqual(self.cls("WGS", "METATRANSCRIPTOMIC", "RANDOM"), "Metatranscriptomics")

    def test_host_transcriptomics(self):
        self.assertEqual(self.cls("RNA-Seq", "TRANSCRIPTOMIC", "cDNA"), "RNA-seq")

    def test_viral_rna(self):
        self.assertEqual(self.cls("WGA", "VIRAL RNA", "PCR", title="SARS-CoV-2 ARTIC amplicon wastewater"), "Viral RNA")
        self.assertEqual(self.cls("AMPLICON", "VIRAL RNA", "PCR"), "Viral RNA")

    def test_marker_genes(self):
        self.assertEqual(self.cls("AMPLICON", "METAGENOMIC", "PCR", title="16S rRNA V4"), "16S")
        self.assertEqual(self.cls("AMPLICON", "METAGENOMIC", "PCR", title="fungal ITS2"), "ITS")
        self.assertEqual(self.cls("AMPLICON", "METAGENOMIC", "PCR", title="18S survey"), "18S")

    def test_english_its_is_not_marker(self):
        # "its" / "sites" previously triggered the ITS class.
        self.assertEqual(self.cls("AMPLICON", "METAGENOMIC", "PCR", title="subway sites and its surfaces"), "Amplicon")

    def test_wgs_strategy_beats_pcr_selection(self):
        self.assertEqual(self.cls("WGS", "METAGENOMIC", "PCR"), "WGS")

    def test_unknown(self):
        self.assertEqual(self.cls("OTHER", "OTHER", "other"), "Unknown")


class GeoTests(unittest.TestCase):
    def test_state_abbrev_not_city(self):
        d = parse_geo_loc_name("USA: Ames, IA")
        self.assertEqual((d["city"], d["region"], d["city_status"]), ("Ames", "Iowa", "parsed"))

    def test_state_only(self):
        for raw in ("USA: Iowa", "USA: AZ", "China: Guangdong Province", "USA: Miami Dade County"):
            d = parse_geo_loc_name(raw)
            self.assertEqual(d["city"], "", raw)
            self.assertEqual(d["city_status"], "region_only", raw)

    def test_placeholders_and_continents(self):
        for raw in ("USA: multiple cities", "Kenya: Africa", "USA: city"):
            self.assertEqual(parse_geo_loc_name(raw)["city"], "", raw)

    def test_city_named_like_state(self):
        self.assertEqual(parse_geo_loc_name("USA: New York, NY")["city"], "New York")

    def test_missing_country(self):
        self.assertEqual(parse_geo_loc_name("missing")["country"], "")

    def test_infer_geo_normalizes_and_keeps_provenance(self):
        g = infer_geo({"accession": "SAMN1", "attributes": {
            "geo_loc_name": "USA: Boston, MA", "lat_lon": "42.36 N 71.06 W", "collection_date": "2021-05-03"}}, [])
        self.assertEqual(g["country"], "United States")
        self.assertEqual(g["city"], "Boston")
        self.assertEqual((g["lat"], g["lon"]), ("42.36", "-71.06"))
        self.assertEqual(g["collection_date"], "2021-05-03")
        self.assertEqual(g["raw"], "USA: Boston, MA")
        self.assertEqual(g["resolution"], "text_parsed_unvalidated")

    def test_country_fallback_needs_word_boundary(self):
        # "uk" inside "Fukuoka" must not yield United Kingdom.
        self.assertEqual(infer_geo({}, ["Fukuoka sewer"])["country"], "")


class NcbiTests(unittest.TestCase):
    EXPXML = ("&lt;Summary&gt;&lt;Title&gt;Subway swab 12&lt;/Title&gt;&lt;/Summary&gt;"
              "&lt;Experiment acc=\"SRX1\" name=\"x\"/&gt;&lt;Study acc=\"SRP1\" name=\"NYC subway\"/&gt;")

    def _docsum_xml(self, uids):
        docs = "".join(
            f'<DocSum><Id>{u}</Id><Item Name="ExpXml" Type="String">{self.EXPXML}</Item></DocSum>' for u in uids)
        return f"<eSummaryResult>{docs}</eSummaryResult>".encode()

    def test_title_from_expxml(self):
        with mock.patch.object(ncbi, "http_get", side_effect=lambda url: self._docsum_xml(["1"])):
            out, _ = ncbi.esummary_sra(["1"])
        self.assertEqual(out["1"]["title"], "Subway swab 12")
        self.assertEqual(out["1"]["study_name"], "NYC subway")
        self.assertEqual(out["1"]["experiment_accession"], "SRX1")

    def test_all_uids_summarized_in_batches(self):
        uids = [str(i) for i in range(1, 1201)]
        calls = []

        def fake_get(url):
            import urllib.parse
            ids = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)["id"][0].split(",")
            calls.append(len(ids))
            return self._docsum_xml(ids)

        with mock.patch.object(ncbi, "http_get", side_effect=fake_get):
            out, _ = ncbi.esummary_sra(uids)
        self.assertEqual(len(out), 1200)
        self.assertTrue(all(n <= ncbi.ESUMMARY_BATCH_SIZE for n in calls))


class DateSearchTests(unittest.TestCase):
    def test_day_search_uses_pdat_and_slash_dates(self):
        # SRA ignores datetype=edat and ISO dashes; both silently return zero hits.
        import urllib.parse
        seen = {}

        def fake_get(url):
            seen.update(urllib.parse.parse_qs(urllib.parse.urlparse(url).query))
            return b"<eSearchResult><IdList/></eSearchResult>"

        with mock.patch.object(ncbi, "http_get", side_effect=fake_get):
            ncbi.esearch_day("sra", "wastewater", "2024-06-12", 10)
        self.assertEqual(seen["datetype"], ["pdat"])
        self.assertEqual(seen["mindate"], ["2024/06/12"])
        self.assertEqual(seen["maxdate"], ["2024/06/12"])

    def test_recent_search_uses_pdat(self):
        import urllib.parse
        seen = {}

        def fake_get(url):
            seen.update(urllib.parse.parse_qs(urllib.parse.urlparse(url).query))
            return b"<eSearchResult><IdList/></eSearchResult>"

        with mock.patch.object(ncbi, "http_get", side_effect=fake_get):
            ncbi.esearch_recent("sra", "wastewater", 7, 10)
        self.assertEqual(seen["datetype"], ["pdat"])


def rec_with(organism, city="", city_status="parsed", project_title=""):
    return {
        "runinfo_row": {"ScientificName": organism},
        "geo": {"city": city, "city_status": city_status},
        "bioproject": {"title": project_title, "description": ""},
    }


def bs(**attrs):
    return {"attributes": attrs, "title": ""}


class EligibilityTests(unittest.TestCase):
    def tier(self, rec, biosample=None):
        return assess_eligibility(rec, biosample or {}, rec["bioproject"])["status"]

    # Stage 2: environmental eligibility
    def test_human_gut_is_non_environmental(self):
        r = rec_with("human gut metagenome", city="New York City")
        self.assertEqual(self.tier(r, bs(host="Homo sapiens", isolation_source="stool")), "non_environmental")

    def test_clinical_blood_in_city_is_non_environmental(self):
        r = rec_with("metagenome", city="Boston")
        self.assertEqual(self.tier(r, bs(isolation_source="human blood")), "non_environmental")

    def test_clinical_isolate_is_non_environmental(self):
        r = rec_with("Staphylococcus aureus", city="Iowa City")
        self.assertEqual(self.tier(r, bs(isolation_source="patient sputum")), "non_environmental")

    def test_lab_material_is_non_environmental(self):
        r = rec_with("bioreactor metagenome", city="Nanjing")
        self.assertEqual(self.tier(r), "non_environmental")

    def test_no_environmental_evidence_is_uncertain(self):
        self.assertEqual(self.tier(rec_with("metagenome", city="Chicago")), "uncertain")

    def test_isolate_from_wastewater_is_uncertain(self):
        r = rec_with("Escherichia coli")
        self.assertEqual(self.tier(r, bs(isolation_source="wastewater")), "uncertain")

    def test_host_field_holding_environment_is_not_a_host(self):
        r = rec_with("wastewater metagenome", city="Zurich")
        self.assertEqual(self.tier(r, bs(host="Wastewater")), "probable_urban")

    # Stage 3: urban origin
    def test_explicit_building_setting_urban(self):
        r = rec_with("metagenome")
        self.assertEqual(self.tier(r, bs(building_setting="urban", isolation_source="surface swab")), "confirmed_urban")

    def test_subway_handrail_is_confirmed(self):
        r = rec_with("metagenome", city="New York City")
        self.assertEqual(self.tier(r, bs(isolation_source="subway handrail")), "confirmed_urban")

    def test_municipal_wastewater_influent_is_confirmed(self):
        r = rec_with("wastewater metagenome")
        self.assertEqual(self.tier(r, bs(isolation_source="municipal wastewater influent")), "confirmed_urban")

    def test_city_alone_gives_probable(self):
        r = rec_with("soil metagenome", city="Chicago")
        self.assertEqual(self.tier(r, bs(isolation_source="soil")), "probable_urban")

    def test_project_context_alone_gives_probable(self):
        r = rec_with("air metagenome", project_title="Microbiome of the New York City subway")
        self.assertEqual(self.tier(r), "probable_urban")

    def test_wastewater_surveillance_project_gives_probable(self):
        r = rec_with("wastewater metagenome", project_title="Wastewater surveillance of respiratory viruses")
        self.assertEqual(self.tier(r), "probable_urban")

    def test_wastewater_without_urban_evidence_is_uncertain(self):
        self.assertEqual(self.tier(rec_with("wastewater metagenome")), "uncertain")

    def test_ambiguous_state_city_is_uncertain(self):
        r = rec_with("freshwater metagenome", city="", city_status="region_only")
        self.assertEqual(self.tier(r, bs(env_medium="freshwater")), "uncertain")

    def test_rural_site_is_non_urban(self):
        r = rec_with("soil metagenome")
        r["geo"]["raw"] = "USA: rural Pennsylvania"
        self.assertEqual(self.tier(r, bs(env_medium="soil")), "non_urban")
        self.assertEqual(self.tier(rec_with("metagenome"), bs(built_struc_set="rural", env_medium="dust")), "non_urban")

    def test_city_in_place_name_is_not_urban_wording(self):
        r = rec_with("farm metagenome")
        self.assertEqual(self.tier(r, bs(isolation_source="camel stable, China: Fukang City, Xinjiang")), "non_urban")
        self.assertEqual(self.tier(rec_with("soil metagenome"), bs(isolation_source="city park soil")), "confirmed_urban")

    def test_swine_wastewater_is_non_urban(self):
        r = rec_with("wastewater metagenome", city="Xinxing", project_title="Metagenomes of swine wastewater")
        self.assertEqual(self.tier(r), "non_urban")

    def test_space_station_is_non_urban(self):
        r = rec_with("indoor metagenome", project_title="Microbial mapping of the ISS environment")
        self.assertEqual(self.tier(r, bs(isolation_source="indoor surface")), "non_urban")

    def test_urban_soil_needs_urban_evidence(self):
        self.assertEqual(self.tier(rec_with("soil metagenome"), bs(env_medium="soil")), "uncertain")
        self.assertEqual(self.tier(rec_with("soil metagenome"), bs(env_broad_scale="urban biome", env_medium="soil")), "confirmed_urban")

    # Stage 4: annotation
    def test_result_records_structured_evidence(self):
        r = rec_with("air metagenome", city="Shanghai")
        r["geo"].update({"raw": "China: Shanghai", "raw_field": "geo_loc_name"})
        out = assess_eligibility(r, bs(isolation_source="subway platform air"), {})
        self.assertEqual(out["status"], "confirmed_urban")
        self.assertIn("transit", out["urban_context"])
        self.assertEqual(out["location_resolution"], "city")
        self.assertEqual(out["location_validation"], "plausible")
        self.assertTrue(out["environmental_sample"])
        self.assertEqual(out["decision_method"], "rules")
        fields = {(e["source"], e["field"], e["signal"]) for e in out["evidence"]}
        self.assertIn(("BioSample", "isolation_source", "urban_infrastructure"), fields)
        self.assertIn(("BioSample", "geo_loc_name", "urban_location"), fields)


class CatalogReadTests(unittest.TestCase):
    def test_part_files_are_read_once(self):
        import json, tempfile
        from umdb_harvester import exports
        from umdb_harvester.utils import iter_jsonl_glob
        with tempfile.TemporaryDirectory() as d:
            for name, runs in (("srr_catalog_2026.jsonl", ["A", "B"]),
                               ("srr_catalog_2026_part000.jsonl", ["C"]),
                               ("srr_catalog_2026_part001.jsonl", ["D"]),
                               ("srr_catalog_2025.jsonl", ["E"])):
                with open(os.path.join(d, name), "w") as fh:
                    fh.writelines(json.dumps({"srr": r}) + "\n" for r in runs)
            with mock.patch.object(exports, "DATA_DIR", d):
                bases = exports._find_year_catalog_prefixes()
            self.assertEqual([os.path.basename(b) for b in bases], ["srr_catalog_2025.jsonl", "srr_catalog_2026.jsonl"])
            runs = [r["srr"] for b in bases for r in iter_jsonl_glob(b)]
            self.assertEqual(sorted(runs), ["A", "B", "C", "D", "E"])


class RefreshTests(unittest.TestCase):
    def _write(self, d, name, recs):
        import json
        with open(os.path.join(d, name), "w") as fh:
            fh.writelines(json.dumps(r) + "\n" for r in recs)

    def test_newest_record_wins_and_compaction_drops_superseded(self):
        import tempfile
        from umdb_harvester import exports
        from umdb_harvester.utils import iter_jsonl_glob
        old = {"srr": "SRR1", "title": "old", "provenance": {"ingested_utc": "2026-01-01T00:00:00Z"}}
        new = {"srr": "SRR1", "title": "new", "provenance": {"ingested_utc": "2026-11-15T00:00:00Z"}}
        other = {"srr": "SRR2", "provenance": {"ingested_utc": "2026-02-01T00:00:00Z"}}
        with tempfile.TemporaryDirectory() as d:
            # The refreshed record lands in an older year's file: newest timestamp must still win.
            self._write(d, "srr_catalog_2026.jsonl", [old, other])
            self._write(d, "srr_catalog_2024.jsonl", [new])
            with mock.patch.object(exports, "DATA_DIR", d):
                bases = exports._find_year_catalog_prefixes()
                winners = exports.latest_record_positions(bases)
                kept = [rec for pos, _, rec in exports._iter_catalog(bases) if winners[rec["srr"]] == pos]
                self.assertEqual(sorted((r["srr"], r.get("title")) for r in kept), [("SRR1", "new"), ("SRR2", None)])
                stats = exports.compact_catalog()
                bases = exports._find_year_catalog_prefixes()
            self.assertEqual(stats, {"records_before": 3, "records_after": 2, "superseded_removed": 1})
            after = [r for b in bases for r in iter_jsonl_glob(b)]
            self.assertEqual(sorted((r["srr"], r.get("title")) for r in after), [("SRR1", "new"), ("SRR2", None)])
            self.assertFalse([n for n in os.listdir(d) if n.startswith("umdb_compact_")])

    def test_day_search_pages_until_count(self):
        import urllib.parse
        calls = []

        def fake_get(url):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
            start, size = int(q["retstart"][0]), int(q["retmax"][0])
            calls.append((start, size))
            ids = "".join(f"<Id>{i}</Id>" for i in range(start, min(start + size, 25000)))
            return f"<eSearchResult><Count>25000</Count><IdList>{ids}</IdList></eSearchResult>".encode()

        with mock.patch.object(ncbi, "http_get", side_effect=fake_get):
            ids, _ = ncbi.esearch_day("sra", "x", "2024-01-01", 0)
            capped, _ = ncbi.esearch_day("sra", "x", "2024-01-01", 500)
        self.assertEqual(len(ids), 25000)
        self.assertEqual(len(set(ids)), 25000)
        self.assertEqual(calls[:3], [(0, 10000), (10000, 10000), (20000, 10000)])
        self.assertEqual(len(capped), 500)

    def test_refresh_reingests_seen_runs_without_duplicating_seen_lists(self):
        from umdb_harvester import ingest
        fake_rows = [{"srr": "SRR9", "sra_uid": "1"}]
        with mock.patch.object(ingest, "build_srr_records_for_sra_uid", return_value=(fake_rows, {})), \
             mock.patch.object(ingest, "fetch_runinfo_for_uids", return_value=({"1": []}, {})), \
             mock.patch.object(ingest, "append_lines") as append_lines:
            seen_sra, seen_srr = {"1"}, {"SRR9"}
            added, report = ingest.ingest_uids_to_srr(
                tag="t", uids=["1"], summaries={}, biosample_cache={}, bp_cache={}, bp_uid_cache={},
                seen_sra=seen_sra, seen_srr=seen_srr, fetch_biosample=False, fetch_bioproject=False,
                debug=False, runinfo_max_rows=0, refresh=False)
            self.assertEqual(added, [])
            added, report = ingest.ingest_uids_to_srr(
                tag="t", uids=["1"], summaries={}, biosample_cache={}, bp_cache={}, bp_uid_cache={},
                seen_sra=seen_sra, seen_srr=seen_srr, fetch_biosample=False, fetch_bioproject=False,
                debug=False, runinfo_max_rows=0, refresh=True)
            self.assertEqual([r["srr"] for r in added], ["SRR9"])
            self.assertEqual(report["counters"].get("srr_refreshed"), 1)
            append_lines.assert_not_called()


RUNINFO_HEADER = "Run,Experiment,BioSample,BioProject,LibraryStrategy,LibrarySource\n"


def _runinfo_for(ids):
    # experiment uid N -> SRXN with runs SRRN1/SRRN2 and sample SAMN<N>; NCBI may
    # repeat the header between IDs in a multi-ID response.
    out = RUNINFO_HEADER
    for i in ids:
        out += f"SRR{i}1,SRX{i},SAMN{i},PRJNA9,WGS,METAGENOMIC\nSRR{i}2,SRX{i},SAMN{i},PRJNA9,WGS,METAGENOMIC\n"
        out += RUNINFO_HEADER
    return out


def _biosample_xml(accs):
    body = "".join(
        f'<BioSample accession="{a}"><Ids><Id db="BioSample">{a}</Id></Ids>'
        f"<Description><Title>Influent wastewater {a}</Title></Description>"
        f'<Attributes><Attribute attribute_name="geo_loc_name">USA: Boston</Attribute>'
        f'<Attribute attribute_name="isolation_source">wastewater treatment plant influent</Attribute></Attributes>'
        f"</BioSample>" for a in accs)
    return f"<BioSampleSet>{body}</BioSampleSet>"


class BatchFetchTests(unittest.TestCase):
    def fake_ncbi(self, calls, fail_batches=False, drop=()):
        import urllib.parse

        def fake_get(url):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
            if "term" in q:  # esearch for accessions efetch did not return
                accs = [t.split("[")[0] for t in q["term"][0].split(" OR ")]
                calls.append(("esearch", len(accs)))
                found = "".join(f"<Id>{9000 + int(a[5:])}</Id>" for a in accs if a.startswith("SAMEA"))
                return f"<eSearchResult><Count>0</Count><IdList>{found}</IdList></eSearchResult>".encode()
            ids = q["id"][0].split(",")
            calls.append((q["db"][0], len(ids)))
            if q["db"][0] == "sra":
                if fail_batches and len(ids) > 1:
                    raise RuntimeError("HTTP 500")
                return _runinfo_for(ids).encode()
            if all(i.isdigit() for i in ids):  # fetch by UID: SAMEA records
                return _biosample_xml([f"SAMEA{int(i) - 9000}" for i in ids]).encode()
            return _biosample_xml([a for a in ids if a not in drop and not a.startswith("SAMEA")]).encode()
        return fake_get

    def test_runinfo_batched_and_matched_by_experiment(self):
        from umdb_harvester import ingest
        calls = []
        uids = [str(i) for i in range(1, 451)]
        summaries = {u: {"experiment_accession": f"SRX{u}"} for u in uids[:-1]}  # last has no accession
        with mock.patch.object(ncbi, "http_get", side_effect=self.fake_ncbi(calls)):
            rows, errors = ingest.fetch_runinfo_for_uids(uids, summaries)
        self.assertEqual(errors, {})
        self.assertEqual(sorted(n for _, n in calls), [1, 49, 200, 200])  # 3 batches + 1 single
        self.assertEqual([r["Run"] for r in rows["7"]], ["SRR71", "SRR72"])
        self.assertEqual(len(rows["450"]), 2)
        self.assertEqual(sum(len(v) for v in rows.values()), 900)  # repeated headers dropped

    def test_failed_runinfo_batch_falls_back_to_single_requests(self):
        from umdb_harvester import ingest
        calls = []
        uids = ["1", "2", "3"]
        summaries = {u: {"experiment_accession": f"SRX{u}"} for u in uids}
        with mock.patch.object(ncbi, "http_get", side_effect=self.fake_ncbi(calls, fail_batches=True)):
            rows, errors = ingest.fetch_runinfo_for_uids(uids, summaries)
        self.assertEqual(errors, {})
        self.assertEqual({u: len(r) for u, r in rows.items()}, {"1": 2, "2": 2, "3": 2})

    def test_biosamples_batched_with_single_fallback(self):
        from umdb_harvester import biosample
        calls, cache = [], {}
        accs = [f"SAMN{i}" for i in range(250)]
        fake = self.fake_ncbi(calls, drop={"SAMN5"})
        with mock.patch.object(ncbi, "http_get", side_effect=fake), \
             mock.patch.object(biosample, "http_get", side_effect=fake):
            biosample.fetch_biosamples(accs, cache)
        self.assertEqual(sorted(calls), [("biosample", 50), ("biosample", 200), ("esearch", 1)])
        self.assertEqual(len(cache), 250)
        self.assertEqual(cache["SAMN3"]["attributes"]["geo_loc_name"], "USA: Boston")
        self.assertEqual(cache["SAMN5"]["attributes"], {})  # NCBI has nothing: cached empty

    def test_samea_accessions_resolved_through_uids(self):
        from umdb_harvester import biosample
        calls, cache = [], {}
        accs = ["SAMN1", "SAMEA7", "SAMEA8"]
        fake = self.fake_ncbi(calls)
        with mock.patch.object(ncbi, "http_get", side_effect=fake), \
             mock.patch.object(biosample, "http_get", side_effect=fake):
            biosample.fetch_biosamples(accs, cache)
        self.assertEqual(calls, [("biosample", 3), ("esearch", 2), ("biosample", 2)])
        self.assertEqual(cache["SAMEA8"]["attributes"]["geo_loc_name"], "USA: Boston")
        self.assertEqual(cache["SAMEA8"]["accession"], "SAMEA8")

    def test_ingest_makes_few_requests_and_fills_records(self):
        from umdb_harvester import ingest, biosample
        calls = []
        uids = [str(i) for i in range(1, 301)]
        summaries = {u: {"uid": u, "title": "influent", "experiment_accession": f"SRX{u}"} for u in uids}
        fake = self.fake_ncbi(calls)
        with mock.patch.object(ncbi, "http_get", side_effect=fake), \
             mock.patch.object(biosample, "http_get", side_effect=fake), \
             mock.patch.object(ingest, "append_lines"):
            added, report = ingest.ingest_uids_to_srr(
                tag="t", uids=uids, summaries=summaries, biosample_cache={}, bp_cache={}, bp_uid_cache={},
                seen_sra=set(), seen_srr=set(), fetch_biosample=True, fetch_bioproject=False,
                debug=False, runinfo_max_rows=0, refresh=False)
        self.assertEqual(len(calls), 4)  # 2 RunInfo + 2 BioSample batches for 300 experiments
        self.assertEqual(len(added), 600)
        self.assertEqual(report["counters"]["sra_uids_processed_ok"], 300)
        rec = added[0]
        self.assertEqual(rec["geo"]["country"], "United States")
        self.assertNotIn("api_key", rec["debug"]["runinfo_url"] + rec["debug"]["biosample_url"])

    def test_bioprojects_batched_and_matched_by_accession(self):
        import urllib.parse
        from umdb_harvester import bioproject
        calls = []

        def fake_get(url):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
            if "term" in q:
                accs = [t.split("[")[0] for t in q["term"][0].split(" OR ")]
                calls.append(("esearch", len(accs)))
                ids = "".join(f"<Id>{a[5:]}</Id>" for a in accs if a != "PRJNA404")
                return f"<eSearchResult><Count>1</Count><IdList>{ids}</IdList></eSearchResult>".encode()
            ids = q["id"][0].split(",")
            calls.append(("esummary", len(ids)))
            docs = "".join(f'<DocumentSummary uid="{i}"><Project_Acc>PRJNA{i}</Project_Acc>'
                           f"<Project_Title>Sewage {i}</Project_Title></DocumentSummary>" for i in ids)
            return f"<eSummaryResult><DocumentSummarySet>{docs}</DocumentSummarySet></eSummaryResult>".encode()

        cache, uids = {}, {"PRJNA7": "7"}
        accs = ["PRJNA7"] + [f"PRJNA{i}" for i in range(100, 160)] + ["PRJNA404"]
        with mock.patch.object(ncbi, "http_get", side_effect=fake_get):
            bioproject.fetch_bioprojects(accs, cache, uids)
        # 61 lookups -> 2 searches; 61 UIDs -> 1 summary; PRJNA404 -> single esearch
        self.assertEqual(calls, [("esearch", 50), ("esearch", 11), ("esummary", 61), ("esearch", 1)])
        self.assertEqual(cache["PRJNA120"]["title"], "Sewage 120")
        self.assertEqual(uids["PRJNA120"], "120")
        self.assertEqual(cache["PRJNA404"]["error"], "uid_not_found")

    def test_rate_recovers_after_quiet_period(self):
        with mock.patch.object(ncbi, "NCBI_RPS", 4.0), mock.patch.object(ncbi, "NCBI_RPS_TARGET", 8.0), \
             mock.patch.object(ncbi, "_LAST_PENALTY", [0.0]), mock.patch.object(ncbi, "_LAST_RECOVER", [0.0]):
            self.assertIsNone(ncbi._maybe_recover(30.0))            # too soon after the 429
            self.assertAlmostEqual(ncbi._maybe_recover(61.0), 4.4)  # first step
            self.assertIsNone(ncbi._maybe_recover(65.0))            # wait between steps
            t = 61.0
            while ncbi.NCBI_RPS < 8.0:
                t += 10
                ncbi._maybe_recover(t)
            self.assertEqual(ncbi.NCBI_RPS, 8.0)

    def test_parallel_map_keeps_order(self):
        self.assertEqual(ncbi.parallel_map(lambda x: x * 2, range(50), workers=8), [x * 2 for x in range(50)])


class SecretTests(unittest.TestCase):
    def test_scrub_removes_key_and_email(self):
        from umdb_harvester.utils import scrub_secrets
        url = "https://e/efetch.fcgi?db=sra&id=1&api_key=abc123&tool=umdb&email=me%40x.org"
        self.assertEqual(scrub_secrets(url), "https://e/efetch.fcgi?db=sra&id=1&tool=umdb")
        self.assertEqual(scrub_secrets("https://e/x?api_key=abc&db=sra"), "https://e/x?db=sra")
        self.assertEqual(scrub_secrets('{"u": "https://e/x?db=1&api_key=k"}'), '{"u": "https://e/x?db=1"}')

    def test_writers_and_fetchers_never_store_the_key(self):
        import json
        import tempfile
        from umdb_harvester.utils import write_json, append_jsonl, scrub_files
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "a.json")
            write_json(p, {"u": "https://e/x?db=1&api_key=SECRET"})
            append_jsonl(os.path.join(d, "b.jsonl"), [{"u": "https://e/x?db=1&api_key=SECRET"}])
            with open(os.path.join(d, "old.jsonl"), "w") as fh:
                fh.write(json.dumps({"u": "https://e/x?db=1&api_key=SECRET&email=a%40b"}) + "\n")
            changed = scrub_files([d])
            blob = "".join(open(os.path.join(d, f)).read() for f in os.listdir(d))
        self.assertNotIn("SECRET", blob)
        self.assertEqual(list(changed.values()), [1])
        with mock.patch.object(ncbi, "NCBI_API_KEY", "SECRET"), \
             mock.patch.object(ncbi, "http_get", return_value=b"Run\n"):
            _, url = ncbi.efetch_runinfo_text("1")
        self.assertNotIn("SECRET", url)


class DateWindowTests(unittest.TestCase):
    def test_windows_cover_the_year_to_today(self):
        import datetime as dt
        from umdb_harvester.cli import date_windows
        today = dt.date(2026, 3, 10)
        months = date_windows(2026, "month", today)
        self.assertEqual(months, [(dt.date(2026, 1, 1), dt.date(2026, 1, 31)),
                                  (dt.date(2026, 2, 1), dt.date(2026, 2, 28)),
                                  (dt.date(2026, 3, 1), dt.date(2026, 3, 10))])
        self.assertEqual(len(date_windows(2026, "day", today)), 69)
        weeks = date_windows(2025, "week", today)
        self.assertEqual(weeks[0], (dt.date(2025, 1, 1), dt.date(2025, 1, 7)))
        self.assertEqual(weeks[-1][1], dt.date(2025, 12, 31))
        self.assertEqual(sum((b - a).days + 1 for a, b in weeks), 365)
        self.assertEqual(date_windows(2027, "month", today), [])


class CatalogBundleTests(unittest.TestCase):
    """Pack/unpack/merge of the catalog bundle stored in Release assets (offline)."""

    def setUp(self):
        import tempfile
        self.tmp = tempfile.TemporaryDirectory()
        self.cwd = os.getcwd()
        os.chdir(self.tmp.name)
        os.makedirs("data/cache")
        self.write("data/srr_catalog_2025.jsonl", '{"srr": "SRR1", "n": 1}\n{"srr": "SRR2", "n": 1}\n')
        self.write("data/cache/biosample.json", '{"SAMN1": {"attributes": {"a": "1"}}}')
        self.write("data/seen_srr_runs.txt", "SRR1\nSRR2\n")

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def write(self, path, text):
        with open(path, "w") as fh:
            fh.write(text)

    def read(self, path):
        with open(path) as fh:
            return fh.read()

    def test_pack_and_restore_round_trip(self):
        from umdb_harvester import catalog
        manifest = catalog.pack("out")
        self.assertEqual(sorted(e["path"] for e in manifest["files"]),
                         ["data/cache/biosample.json", "data/seen_srr_runs.txt", "data/srr_catalog_2025.jsonl"])
        original = self.read("data/srr_catalog_2025.jsonl")
        self.write("data/srr_catalog_2025.jsonl", "garbage\n")
        self.write("data/srr_catalog_2019.jsonl", '{"srr": "OLD"}\n')  # not in the bundle: removed on restore
        catalog.unpack("out/umdb-catalog.tar.gz", manifest)
        self.assertEqual(self.read("data/srr_catalog_2025.jsonl"), original)
        self.assertFalse(os.path.exists("data/srr_catalog_2019.jsonl"))

    def test_merge_keeps_local_records_and_adds_published_ones(self):
        from umdb_harvester import catalog
        manifest = catalog.pack("out")
        # Local harvest since then: one new run, one new cache entry, one more seen run.
        self.write("data/srr_catalog_2025.jsonl", '{"srr": "SRR1", "n": 1}\n{"srr": "SRR3", "n": 2}\n')
        self.write("data/cache/biosample.json", '{"SAMN9": {"attributes": {}}}')
        self.write("data/seen_srr_runs.txt", "SRR1\nSRR3\n")
        catalog.unpack("out/umdb-catalog.tar.gz", manifest, merge=True)
        runs = [json.loads(x)["srr"] for x in self.read("data/srr_catalog_2025.jsonl").splitlines()]
        self.assertEqual(sorted(runs), ["SRR1", "SRR2", "SRR3"])
        self.assertEqual(sorted(json.loads(self.read("data/cache/biosample.json"))), ["SAMN1", "SAMN9"])
        self.assertEqual(sorted(self.read("data/seen_srr_runs.txt").split()), ["SRR1", "SRR2", "SRR3"])

    def test_tampered_bundle_is_rejected(self):
        from umdb_harvester import catalog
        manifest = catalog.pack("out")
        manifest["bundle_sha256"] = "0" * 64
        with self.assertRaises(RuntimeError):
            catalog.unpack("out/umdb-catalog.tar.gz", manifest)


class ReleaseTests(unittest.TestCase):
    def _build(self, d):
        import json
        from umdb_harvester import database, exports
        DatabaseTests._catalog(None, d)
        # SAMN1 also has an amplicon run in a second project: one BioSample,
        # two assays, two projects. It must still be counted once.
        extra = {"srr": "SRR3", "sra_uid": "3", "title": "subway 16S",
                 "runinfo_row": {"Run": "SRR3", "Experiment": "SRX3", "BioSample": "SAMN1", "BioProject": "PRJNA2",
                                 "ScientificName": "metagenome", "LibraryStrategy": "AMPLICON",
                                 "LibrarySource": "METAGENOMIC", "ReleaseDate": "2025-01-01", "bases": "1000"},
                 "provenance": {"ingested_utc": "2026-01-01T00:00:00Z"}}
        with open(os.path.join(d, "srr_catalog_2026.jsonl"), "a") as fh:
            fh.write(json.dumps(extra) + "\n")
        dbp = os.path.join(d, "umdb.sqlite")
        with mock.patch.object(exports, "DATA_DIR", d), \
             mock.patch.object(database, "BIOSAMPLE_CACHE", os.path.join(d, "cache", "biosample.json")), \
             mock.patch.object(database, "BIOPROJECT_CACHE", os.path.join(d, "cache", "bioproject.json")), \
             mock.patch.object(database, "AI_CURATION_CACHE", os.path.join(d, "cache", "ai.json")):
            database.build_database(dbp, geonames_dir=make_gazetteer(os.path.join(d, "geonames")))
        return dbp

    @staticmethod
    def _striga_results(d, db_sha):
        """A minimal STRIGA results folder (contract striga-results/1) for a database."""
        res = os.path.join(d, "results")
        os.makedirs(os.path.join(res, "hypotheses", "C1"))
        os.makedirs(os.path.join(res, "snapshot"))
        os.makedirs(os.path.join(res, "pipeline_info"))
        json.dump({"contract": "striga-results/1", "pipeline": {"name": "STRIGA", "version": "0.2.0", "commit": "abc1234"},
                   "input": {"database_sha256": db_sha, "built_utc": "2026-01-01", "rows": {"samples": 2}},
                   "created_utc": "2026-11-20T00:00:00Z",
                   "hypotheses": [{"id": "C1", "track": "catalog", "status": "supported"}]},
                  open(os.path.join(res, "striga_manifest.json"), "w"))
        with open(os.path.join(res, "hypotheses_status.tsv"), "w") as fh:
            fh.write("id\ttrack\tprogram\ttitle\tstatus\tresult\nC1\tcatalog\tUMDB-GEO\tCoverage\tsupported\tok\n")
        for rel in ("report.md", "hypotheses/C1/summary.json", "snapshot/snapshot.json", "snapshot/samples.tsv.gz",
                    "pipeline_info/trace.txt"):
            open(os.path.join(res, rel), "w").write("{}")
        return res

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "releases need pyarrow (requirements-release.txt)")
    def test_release_contract_with_striga_analysis(self):
        """The release UMA and STRIGA read: contract, reference data, templates and verified STRIGA results."""
        import hashlib, tarfile, tempfile
        from umdb_harvester import release
        with tempfile.TemporaryDirectory() as d:
            dbp = self._build(d)
            sha = hashlib.sha256(open(dbp, "rb").read()).hexdigest()
            res = self._striga_results(d, sha)
            geo = os.path.join(d, "geonames")
            out = release.write_release("9.9.9", db_path=dbp, out_root=os.path.join(d, "dist"),
                                        analysis_dir=res, geonames_dir=geo)
            m = json.load(open(os.path.join(out, "manifest.json")))
            self.assertEqual(m["contract"], "umdb-release/1")
            self.assertEqual(m["analysis"]["pipeline"]["version"], "0.2.0")
            self.assertTrue(m["analysis"]["database_verified"])
            for rel in ("reference/geonames/countryInfo.txt", "documentation/cohort-templates.json",
                        "collections/collections.json", "indexes/coverage.json", "analysis/striga_manifest.json",
                        "analysis/hypotheses/C1/summary.json", "analysis/snapshot/snapshot.json"):
                self.assertIn(rel, m["files"], rel)
                self.assertTrue(os.path.exists(os.path.join(out, rel)), rel)
            # the snapshot tables duplicate the catalog, and Nextflow's run records are not results
            self.assertFalse(os.path.exists(os.path.join(out, "analysis", "snapshot", "samples.tsv.gz")))
            self.assertFalse(os.path.exists(os.path.join(out, "analysis", "pipeline_info")))
            self.assertTrue(any(n.startswith("umdb.sqlite.") for n in os.listdir(out)))
            with tarfile.open(os.path.join(d, "dist", "umdb-9.9.9.tar.gz")) as tf:
                self.assertIn("umdb-9.9.9/manifest.json", tf.getnames())
            # every file in checksums.sha256 verifies
            for line in open(os.path.join(out, "checksums.sha256")):
                digest, name = line.split(None, 1)
                self.assertEqual(hashlib.sha256(open(os.path.join(out, name.strip()), "rb").read()).hexdigest(), digest)

    def test_release_refuses_results_from_another_database(self):
        import tempfile
        from umdb_harvester import release
        with tempfile.TemporaryDirectory() as d:
            dbp = self._build(d)
            with self.assertRaisesRegex(RuntimeError, "different database"):
                release.check_analysis(self._striga_results(d, "0" * 64), dbp)
            unhashed = self._striga_results(os.path.join(d, "x"), None)
            with self.assertRaisesRegex(RuntimeError, "did not hash"):
                release.check_analysis(unhashed, dbp)
            self.assertEqual(release.check_analysis(unhashed, dbp, allow_unverified=True)["contract"], "striga-results/1")

    def test_site_from_database(self):
        import sqlite3, tempfile
        from umdb_harvester import site
        with tempfile.TemporaryDirectory() as d:
            dbp, out = self._build(d), os.path.join(d, "site")
            os.makedirs(out)
            with open(os.path.join(out, "srr_records_part000.json"), "w") as fh:
                fh.write("[]")
            manifest = site.build_site(dbp, out)
            load = lambda n: json.load(open(os.path.join(out, n)))
            cube, samples, geo, summary = load("cube.json"), load("samples.json"), load("geo.json"), load("summary.json")
            con = sqlite3.connect(dbp)
            q = lambda sql: con.execute(sql).fetchone()[0]

            # Totals from the two cubes equal exact SQLite counts.
            rix = {k: i for i, k in enumerate(cube["cols"])}
            six = {k: i for i, k in enumerate(samples["cols"])}
            self.assertEqual(sum(r[rix["runs"]] for r in cube["rows"]), q("SELECT count(*) FROM runs"))
            n_samples = q("SELECT count(DISTINCT sample_accession) FROM runs")
            self.assertEqual(sum(r[six["samples"]] for r in samples["rows"]), n_samples)
            self.assertEqual((summary["totalRuns"], summary["totalBioSamples"], summary["totalProjects"]), (3, 2, 2))
            self.assertNotIn("samples", cube["cols"])          # runs cube no longer carries sample counts

            # The two-assay, two-project sample sits in one row, with set-valued dims.
            D = samples["dims"]
            multi = [r for r in samples["rows"] if len(D["assaysets"][r[six["assays"]]]) == 2]
            self.assertEqual(len(multi), 1)
            self.assertEqual(multi[0][six["samples"]], 1)
            self.assertEqual(len(D["projsets"][multi[0][six["projects"]]]), 2)

            # Geography comes from the validated table: NYC for SAMN1, none for SAMN2.
            place_names = [p[1] for p in geo["places"]]
            self.assertEqual(place_names, ["New York City"])
            self.assertEqual([c[0] for c in geo["countries"]], ["US"])
            us = [r for r in samples["rows"] if r[six["country"]] == 0]
            self.assertEqual(sum(r[six["samples"]] for r in us),
                             q("SELECT count(*) FROM sample_locations WHERE country_iso2='US' "
                               "AND country_status IN ('verified','probable')"))
            self.assertEqual(summary["countries"][0]["biosamples"], 1)
            self.assertEqual(summary["countries"][0]["runs"], 2)
            self.assertIn("GeoNames", geo["attribution"])

            projects = load("projects.json")
            acc = projects["rows"][0][0]
            shard = load(os.path.join("p", site.shard_name(acc)))
            self.assertEqual(len(shard["runs"]), projects["rows"][0][5])
            sc = {k: i for i, k in enumerate(shard["sample_cols"])}
            s1 = [s for s in shard["samples"] if s[0] == "SAMN1"][0]
            self.assertEqual((s1[sc["city"]], s1[sc["country_iso2"]], s1[sc["geo_loc_raw"]]),
                             ("New York City", "US", "USA: New York City"))
            self.assertFalse(os.path.exists(os.path.join(out, "srr_records_part000.json")))  # legacy removed
            mtime = os.path.getmtime(os.path.join(out, "p", site.shard_name(acc)))
            site.build_site(dbp, out)  # unchanged content is not rewritten
            self.assertEqual(os.path.getmtime(os.path.join(out, "p", site.shard_name(acc))), mtime)
            self.assertEqual(manifest["counts"]["biosamples"], n_samples)

    def test_database_enforces_location_consistency(self):
        import sqlite3, tempfile
        with tempfile.TemporaryDirectory() as d:
            con = sqlite3.connect(self._build(d))
            con.execute("PRAGMA foreign_keys = ON")
            self.assertEqual(con.execute("PRAGMA foreign_key_check").fetchall(), [])
            row = con.execute("SELECT l.country_iso2, p.name, l.status FROM sample_locations l "
                              "JOIN places p ON p.geonames_id = l.place_id WHERE l.sample_accession='SAMN1'").fetchone()
            self.assertEqual(row, ("US", "New York City", "verified"))
            con.execute("INSERT INTO samples(accession) VALUES ('SAMN9')")
            bad = [  # a US place filed under China; a place without an accepted status; verified without a place
                ("SAMN9", "CN", "verified", None, 5128581, "verified"),
                ("SAMN9", "US", "verified", None, 5128581, "ambiguous"),
                ("SAMN9", "US", "verified", None, None, "verified"),
            ]
            for r in bad:
                with self.assertRaises(sqlite3.IntegrityError, msg=r):
                    con.execute("INSERT INTO sample_locations(sample_accession, country_iso2, country_status, "
                                "admin1_code, place_id, status, geo_rules_version) VALUES (?,?,?,?,?,?,'t')", r)


class SlimRecordTests(unittest.TestCase):
    def test_slim_drops_repeated_text_and_keeps_site_fields(self):
        from umdb_harvester.utils import slim_record
        rec = {"srr": "SRR1",
               "bioproject": {"accession": "PRJNA1", "uid": "9", "title": "Subway", "center_name": "C",
                              "description": "long abstract " * 50, "esummary_items": {"x": "y"},
                              "ncbi": {"bioproject_url": "u"}},
               "debug": {"runinfo_url": "r", "runinfo_columns": ["Run"] * 47, "biosample_url": "b"}}
        out = slim_record(rec)
        self.assertEqual(out["bioproject"], {"accession": "PRJNA1", "uid": "9", "title": "Subway", "center_name": "C"})
        self.assertEqual(out["debug"], {"runinfo_url": "r", "biosample_url": "b"})
        self.assertEqual(slim_record(out), out)          # idempotent
        self.assertIn("description", rec["bioproject"])  # input untouched

    def test_export_uses_cached_project_text_for_eligibility(self):
        from umdb_harvester.exports import refresh_derived_annotations
        rec = {"srr": "SRR1", "title": "", "runinfo_row": {"BioProject": "PRJNA1", "ScientificName": "air metagenome"},
               "bioproject": {"accession": "PRJNA1", "title": "Air study"}, "geo": {}}
        bp_cache = {"PRJNA1": {"accession": "PRJNA1", "title": "Air study",
                               "description": "Bioaerosols sampled across the New York City subway system"}}
        without = refresh_derived_annotations(rec, {}, {})
        with_cache = refresh_derived_annotations(rec, {}, bp_cache)
        self.assertEqual(without["eligibility"]["status"], "uncertain")
        self.assertEqual(with_cache["eligibility"]["status"], "probable_urban")
        self.assertNotIn("description", with_cache["bioproject"])
        self.assertTrue(all("value" not in e for e in with_cache["eligibility"]["evidence"] if e["source"] == "BioProject"))


class DatabaseTests(unittest.TestCase):
    def _catalog(self, d):
        import json
        os.makedirs(os.path.join(d, "cache"))
        recs = [
            {"srr": "SRR1", "sra_uid": "1", "title": "subway swab",
             "runinfo_row": {"Run": "SRR1", "Experiment": "SRX1", "BioSample": "SAMN1", "BioProject": "PRJNA1",
                             "ScientificName": "metagenome", "LibraryStrategy": "WGS", "LibrarySource": "METAGENOMIC",
                             "ReleaseDate": "2024-05-01 10:00:00", "spots": "1000", "bases": "150000"},
             "provenance": {"ingested_utc": "2026-01-01T00:00:00Z"}},
            {"srr": "SRR1", "sra_uid": "1", "title": "subway swab (refreshed)",
             "runinfo_row": {"Run": "SRR1", "Experiment": "SRX1", "BioSample": "SAMN1", "BioProject": "PRJNA1",
                             "ScientificName": "metagenome", "LibraryStrategy": "WGS", "LibrarySource": "METAGENOMIC",
                             "ReleaseDate": "2024-05-01 10:00:00", "spots": "2000", "bases": "300000"},
             "provenance": {"ingested_utc": "2026-11-15T00:00:00Z"}},
            {"srr": "SRR2", "sra_uid": "2", "title": "stool",
             "runinfo_row": {"Run": "SRR2", "Experiment": "SRX2", "BioSample": "SAMN2", "BioProject": "PRJNA1",
                             "ScientificName": "human gut metagenome", "ReleaseDate": "2024-05-02"},
             "provenance": {"ingested_utc": "2026-01-01T00:00:00Z"}},
        ]
        with open(os.path.join(d, "srr_catalog_2026.jsonl"), "w") as fh:
            fh.writelines(json.dumps(r) + "\n" for r in recs)
        json.dump({"SAMN1": {"accession": "SAMN1", "attributes": {
                        "isolation_source": "subway station handrail", "geo_loc_name": "USA: New York City",
                        "collection_date": "2019-06-04"}},
                   "SAMN2": {"accession": "SAMN2", "attributes": {"isolation_source": "stool", "host": "Homo sapiens",
                                                                   "collection_date": "2019"}}},
                  open(os.path.join(d, "cache", "biosample.json"), "w"))
        json.dump({"PRJNA1": {"accession": "PRJNA1", "title": "NYC subway", "description": "Subway surfaces"}},
                  open(os.path.join(d, "cache", "bioproject.json"), "w"))

    def test_build_query_and_newest_record(self):
        import sqlite3, tempfile
        from umdb_harvester import database, exports, config
        with tempfile.TemporaryDirectory() as d:
            self._catalog(d)
            db = os.path.join(d, "umdb.sqlite")
            with mock.patch.object(exports, "DATA_DIR", d), \
                 mock.patch.object(database, "BIOSAMPLE_CACHE", os.path.join(d, "cache", "biosample.json")), \
                 mock.patch.object(database, "BIOPROJECT_CACHE", os.path.join(d, "cache", "bioproject.json")), \
                 mock.patch.object(database, "AI_CURATION_CACHE", os.path.join(d, "cache", "ai.json")):
                counts = database.build_database(db, geonames_dir=make_gazetteer(os.path.join(d, "geonames")))
            self.assertEqual((counts["runs"], counts["samples"], counts["projects"]), (2, 2, 1))
            con = sqlite3.connect(db)
            self.assertEqual(con.execute("SELECT spots FROM runs WHERE accession='SRR1'").fetchone()[0], 2000)
            s1 = con.execute("SELECT urban_status, urban_context, collection_date, collection_year, city "
                             "FROM samples WHERE accession='SAMN1'").fetchone()
            self.assertEqual(s1, ("confirmed_urban", "transit", "2019-06-04", 2019, "New York City"))
            s2 = con.execute("SELECT urban_status, collection_date, collection_year FROM samples WHERE accession='SAMN2'").fetchone()
            self.assertEqual(s2, ("non_environmental", None, 2019))
            hits = con.execute("SELECT accession FROM search WHERE search MATCH 'handrail'").fetchall()
            self.assertEqual(hits, [("SAMN1",)])
            self.assertEqual(con.execute("SELECT count(*) FROM umdb_runs").fetchone()[0], 2)
            self.assertEqual(con.execute("PRAGMA foreign_key_check").fetchall(), [])
            # With 2 rows the planner rightly scans; check the indexes exist instead.
            indexes = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='index'")}
            self.assertTrue({"idx_samples_cohort", "idx_samples_urban", "idx_runs_sample"} <= indexes)
            con.close()

    def test_collection_date_parsing(self):
        from umdb_harvester.database import parse_collection_date
        self.assertEqual(parse_collection_date("2019-06-04"), ("2019-06-04", 2019))
        self.assertEqual(parse_collection_date("2019-06"), (None, 2019))
        self.assertEqual(parse_collection_date("2019/06/04"), ("2019-06-04", 2019))
        self.assertEqual(parse_collection_date("missing"), (None, None))
        self.assertEqual(parse_collection_date("Jun-2019"), (None, None))



def make_gazetteer(d):
    """A tiny GeoNames extract with the cases the resolver must get right."""
    import zipfile
    os.makedirs(d, exist_ok=True)
    countries = [("US", "USA", "840", "United States", "NA"), ("CN", "CHN", "156", "China", "AS"),
                 ("HK", "HKG", "344", "Hong Kong", "AS"), ("DK", "DNK", "208", "Denmark", "EU"),
                 ("CA", "CAN", "124", "Canada", "NA"), ("GB", "GBR", "826", "United Kingdom", "EU")]
    with open(os.path.join(d, "countryInfo.txt"), "w") as fh:
        fh.write("#ISO\tISO3\tISO-Numeric\tfips\tCountry\tCapital\tArea\tPop\tContinent\n")
        for iso2, iso3, num, name, cont in countries:
            fh.write("\t".join([iso2, iso3, num, "", name, "", "0", "0", cont]) + "\n")
    admin1 = [("US.NY", "New York"), ("US.OR", "Oregon"), ("US.ME", "Maine"), ("US.IL", "Illinois"),
              ("US.MA", "Massachusetts"), ("CN.22", "Beijing"), ("CA.10", "Quebec"), ("DK.17", "Capital Region")]
    with open(os.path.join(d, "admin1CodesASCII.txt"), "w") as fh:
        for i, (code, name) in enumerate(admin1):
            fh.write(f"{code}\t{name}\t{name}\t{1000 + i}\n")
    #   id, name, alternates, lat, lon, code, country, admin1, population
    places = [(5128581, "New York City", "New York,NYC", 40.71427, -74.00597, "PPL", "US", "NY", 8804190),
              (5128638, "Manhattan", "", 40.78343, -73.96625, "PPLX", "US", "NY", 1600000),
              (5110629, "Buffalo", "", 42.88645, -78.87837, "PPLA2", "US", "NY", 278349),
              (5746545, "Portland", "", 45.52345, -122.67621, "PPLA2", "US", "OR", 652503),
              (4975802, "Portland", "", 43.66147, -70.25533, "PPLA2", "US", "ME", 66881),
              (4250542, "Springfield", "", 39.80172, -89.64371, "PPLA", "US", "IL", 114394),
              (4951788, "Springfield", "", 42.10148, -72.58981, "PPLA2", "US", "MA", 155929),
              (1816670, "Beijing", "Peking", 39.9075, 116.39723, "PPLC", "CN", "22", 18960744),
              (1819729, "Hong Kong", "", 22.27832, 114.17469, "PPLC", "HK", "", 7491609),
              (6325494, "Québec", "Quebec,Quebec City", 46.81228, -71.21454, "PPLA", "CA", "10", 531902),
              (2618425, "Copenhagen", "København", 55.67594, 12.56553, "PPLC", "DK", "17", 1153615),
              (2614030, "Søvang", "", 55.58, 12.54, "PPL", "DK", "17", 900),
              (2624000, "Bælum", "", 56.83, 10.11, "PPL", "DK", "", 702),
              (5800000, "Norfolk County", "", 42.85, -80.27, "PPL", "CA", "", 60847)]
    lines = []
    for pid, name, alts, lat, lon, code, cc, a1, pop in places:
        f = [str(pid), name, name, alts, str(lat), str(lon), "P", code, cc, "", a1, "", "", "", str(pop), "", "0", "", ""]
        lines.append("\t".join(f))
    with zipfile.ZipFile(os.path.join(d, "cities500.zip"), "w") as zf:
        zf.writestr("cities500.txt", "\n".join(lines) + "\n")
    return d


class GazetteerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import tempfile
        from umdb_harvester import geo
        cls.tmp = tempfile.TemporaryDirectory()
        cls.gaz = geo.Gazetteer(make_gazetteer(cls.tmp.name))

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def res(self, raw, country, lat=None, lon=None, source="biosample"):
        from umdb_harvester import geo
        a = geo.Resolver(self.gaz).resolve(raw, country, source, lat, lon)
        name = self.gaz.p_name[a["place_index"]] if a["place_index"] is not None else None
        return a, name

    def test_unique_city_is_verified(self):
        a, name = self.res("USA: Buffalo, NY", "United States")
        self.assertEqual((a["status"], name, a["admin1_code"], a["country_iso2"]), ("verified", "Buffalo", "US.NY", "US"))

    def test_city_named_like_its_state(self):
        a, name = self.res("USA: New York, NY", "United States")
        self.assertEqual((a["status"], name), ("probable", "New York City"))
        a, name = self.res("USA: NYC", "United States")       # alternate name
        self.assertEqual((a["status"], name), ("verified", "New York City"))

    def test_city_sections_are_not_places(self):
        a, name = self.res("USA: New York, Manhattan", "United States")
        self.assertNotEqual(name, "Manhattan")

    def test_homonyms(self):
        a, name = self.res("USA: Portland, ME", "United States")
        self.assertEqual((a["status"], self.gaz.p_admin1[a["place_index"]]), ("verified", "US.ME"))
        a, name = self.res("USA: Portland", "United States")       # 10x larger: dominant
        self.assertEqual((a["status"], a["admin1_code"], a["method"]), ("probable", "US.OR", "text-population"))
        a, name = self.res("USA: Springfield", "United States")   # comparable: no guess
        self.assertEqual((a["status"], name), ("ambiguous", None))

    def test_coordinates_confirm_or_contradict(self):
        a, name = self.res("USA: Buffalo, NY", "United States", 42.90, -78.85)
        self.assertEqual((a["status"], a["method"]), ("verified", "text+coordinates"))
        a, name = self.res("USA: Buffalo, NY", "United States", 40.71, -74.006)   # NYC coordinates
        self.assertEqual((a["status"], name), ("conflicting", None))
        a, name = self.res("USA: Buffalo, NY", "United States", 41.0, -74.0)      # whole degrees: too coarse
        self.assertEqual(a["status"], "verified")

    def test_coordinates_alone_pick_the_city_not_the_village(self):
        a, name = self.res("Denmark", "Denmark", 55.6, 12.55)
        self.assertEqual((a["status"], name, a["method"]), ("probable", "Copenhagen", "coordinates"))

    def test_coordinates_alone_need_the_point_inside_the_place(self):
        a, name = self.res("Denmark", "Denmark", 56.833, 10.25)       # 8 km from a 700-person village
        self.assertEqual((a["status"], name, a["country_iso2"]), ("unresolved", None, "DK"))
        a, name = self.res("Denmark", "Denmark", 56.835, 10.115)      # inside it
        self.assertEqual((a["status"], name), ("probable", "Bælum"))
        a, name = self.res("Canada: Ontario", "Canada", 42.85, -80.27) # a county is not a city
        self.assertEqual(name, None)

    def test_coordinates_in_another_country(self):
        a, name = self.res("Canada: Quebec", "Canada", 55.676, 12.566)            # Copenhagen
        self.assertEqual((a["country_status"], a["place_index"]), ("conflicting", None))

    def test_city_states_and_territories(self):
        a, name = self.res("Hong Kong", "Hong Kong")
        self.assertEqual((a["country_iso2"], name, a["method"]), ("HK", "Hong Kong", "city-state"))
        a, name = self.res("China: Hong Kong", "China")
        self.assertEqual((a["country_iso2"], name), ("HK", "Hong Kong"))

    def test_no_country_no_place(self):
        for raw in ("R10016", "Pacific Ocean", "missing: data agreement established pre-2023", ""):
            a, name = self.res(raw, raw)
            self.assertEqual((a["country_iso2"], a["country_status"], name), (None, "unresolved", None), raw)

    def test_text_fallback_country_is_only_probable(self):
        a, _ = self.res("", "China", source="text_fallback")
        self.assertEqual(a["country_status"], "probable")

    def test_place_always_inside_its_country(self):
        from umdb_harvester import geo
        r = geo.Resolver(self.gaz)
        cases = [("USA: Buffalo, NY", "United States", None, None), ("Denmark", "Denmark", 55.6, 12.55),
                 ("Hong Kong", "Hong Kong", None, None), ("USA: Portland", "United States", None, None)]
        for raw, c, lat, lon in cases:
            a = r.resolve(raw, c, "biosample", lat, lon)
            if a["place_index"] is not None:
                self.assertEqual(self.gaz.p_country[a["place_index"]], a["country_iso2"])


class QueryProfileTests(unittest.TestCase):
    def test_urban_compartments_not_excluded(self):
        for name, q in QUERY_PROFILES.items():
            not_clause = q.split(" NOT ", 1)[1].lower() if " NOT " in q else ""
            for term in ("soil", "sediment", "freshwater", "river", "lake", " plant ", "hospital"):
                self.assertNotIn(term, not_clause, f"{name} excludes {term.strip()}")

    def test_defaults_are_the_seven_environment_profiles(self):
        from umdb_harvester.config import DEFAULT_QUERY_PROFILE_NAMES
        self.assertEqual(len(DEFAULT_QUERY_PROFILE_NAMES), 7)
        self.assertNotIn("wastewater_sars_cov_2", DEFAULT_QUERY_PROFILE_NAMES)
        self.assertTrue(set(DEFAULT_QUERY_PROFILE_NAMES) <= set(QUERY_PROFILES))

    def test_no_terms_ncbi_mishandles(self):
        import re
        for name, q in QUERY_PROFILES.items():
            # NCBI drops "ITS" as a stopword and truncates short wildcards to 600 variants.
            self.assertIsNone(re.search(r"\bITS\b(?![12])", q), f"{name} relies on bare ITS")
            self.assertNotIn("metagenom*", q, f"{name} uses a truncated wildcard")

    def test_no_bare_catch_all_terms(self):
        import re
        for name, q in QUERY_PROFILES.items():
            unquoted = re.sub(r'"[^"]*"', " ", q)   # quoted phrases are fine
            for term in ("surface", "swab", "station", "air", "bus", "train"):
                self.assertIsNone(re.search(rf"\b{term}\b", unquoted, re.I), f"{name} uses bare '{term}'")


if __name__ == "__main__":
    unittest.main()
