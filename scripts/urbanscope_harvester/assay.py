"""Rule-based assay classification from SRA library fields.

Classes (assay_class):
  WGS                  shotgun DNA sequencing (metagenomic or genomic)
  Metatranscriptomics  community RNA (LibrarySource METATRANSCRIPTOMIC)
  RNA-seq              single-organism / host transcriptomics (TRANSCRIPTOMIC)
  Viral RNA            targeted viral RNA sequencing (LibrarySource VIRAL RNA),
                       e.g. SARS-CoV-2 tiled-amplicon wastewater surveillance
  16S / 18S / ITS      marker-gene amplicon sequencing
  Amplicon             amplicon sequencing with no identifiable marker
  Unknown

Structured SRA fields (LibraryStrategy / LibrarySource) are trusted over free
text; free text only breaks ties and identifies marker genes.
"""
from __future__ import annotations
import re
from typing import Any, Dict, List

from .utils import _norm

SHOTGUN_STRATEGIES = {"wgs", "wga", "wcs", "targeted-capture", "hi-c", "poolclone", "clone", "wxs", "synthetic-long-read"}
RNA_STRATEGIES = {"rna-seq", "transcriptome", "mirna-seq", "ncrna-seq", "est"}

RE_16S = re.compile(r"\b16\s?s\b", re.I)
RE_18S = re.compile(r"\b18\s?s\b", re.I)
# Case-sensitive "ITS" so the English word "its" is not a match.
RE_ITS = re.compile(r"\bITS[12]?\b")
RE_ITS_LONG = re.compile(r"internal transcribed spacer", re.I)
RE_AMPLICON = re.compile(r"\bamplicons?\b|\bmarker[ -]gene\b|\bV\d(?:-V\d)?\b", re.I)


def _result(assay_class: str, tags: List[str], confidence: str, rationale: List[str]) -> Dict[str, Any]:
    return {"assay_class": assay_class, "assay_tags": tags, "confidence": confidence, "rationale": rationale}


def _marker(raw_blob: str) -> str:
    if RE_16S.search(raw_blob):
        return "16S"
    if RE_18S.search(raw_blob):
        return "18S"
    if RE_ITS.search(raw_blob) or RE_ITS_LONG.search(raw_blob):
        return "ITS"
    return ""


def classify_assay(run_row: Dict[str, str], sra_title: str, biosample_details: Dict[str, Any]) -> Dict[str, Any]:
    attrs = (biosample_details or {}).get("attributes", {}) if isinstance(biosample_details, dict) else {}
    raw_blob = " | ".join([
        sra_title or "",
        run_row.get("LibraryName", "") or "",
        " ".join(f"{k}:{v}" for k, v in attrs.items()),
    ])
    blob = _norm(raw_blob)

    strat = _norm(run_row.get("LibraryStrategy", ""))
    src = _norm(run_row.get("LibrarySource", ""))
    sel = _norm(run_row.get("LibrarySelection", ""))

    # 1. Explicit amplicon strategy: marker-gene study.
    if strat == "amplicon":
        if src == "viral rna":
            return _result("Viral RNA", ["amplicon", "viral"], "high", ["strategy=AMPLICON", "source=VIRAL RNA"])
        marker = _marker(raw_blob)
        if marker:
            return _result(marker, ["amplicon", marker], "high", ["strategy=AMPLICON", f"marker={marker}"])
        return _result("Amplicon", ["amplicon"], "high", ["strategy=AMPLICON"])

    # 2. Library source is the most reliable discriminator for RNA.
    if src == "viral rna":
        tags = ["viral"] + (["amplicon"] if RE_AMPLICON.search(raw_blob) or sel == "pcr" else [])
        return _result("Viral RNA", tags, "high", ["source=VIRAL RNA", f"strategy={strat or '?'}"])
    if src == "metatranscriptomic":
        return _result("Metatranscriptomics", ["RNA", "community"], "high", ["source=METATRANSCRIPTOMIC"])
    if src == "transcriptomic" or strat in RNA_STRATEGIES:
        if "metatranscriptom" in blob:
            return _result("Metatranscriptomics", ["RNA", "community"], "medium", [f"strategy={strat}", "text=metatranscriptom*"])
        return _result("RNA-seq", ["RNA"], "high", [f"strategy={strat or '?'}", f"source={src or '?'}"])

    # 3. Shotgun DNA strategies.
    if strat in SHOTGUN_STRATEGIES:
        tags = ["shotgun"] + ([strat] if strat != "wgs" else [])
        return _result("WGS", tags, "high", [f"strategy={strat}", f"source={src or '?'}"])

    # 4. Strategy missing/OTHER: fall back on selection and free text.
    if src == "metagenomic" and sel in ("random", "random pcr", "unspecified", ""):
        return _result("WGS", ["shotgun"], "medium", ["source=METAGENOMIC", f"selection={sel or '?'}"])
    if sel == "pcr" or RE_AMPLICON.search(raw_blob):
        marker = _marker(raw_blob)
        rationale = [f"selection={sel or '?'}", "text=amplicon" if RE_AMPLICON.search(raw_blob) else "selection=PCR"]
        if marker:
            return _result(marker, ["amplicon", marker], "medium", rationale + [f"marker={marker}"])
        return _result("Amplicon", ["amplicon"], "medium", rationale)
    if "metatranscriptom" in blob:
        return _result("Metatranscriptomics", ["RNA", "community"], "medium", ["text=metatranscriptom*"])
    if src == "metagenomic" or "shotgun" in blob or "metagenom" in blob:
        return _result("WGS", ["shotgun"], "medium", ["source/text=metagenomic"])

    return _result("Unknown", [], "low", [])
