from __future__ import annotations

import json
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

from .config import OPENAI_API_KEY, OPENAI_BASE_URL, OPENAI_MODEL
from .utils import _norm, _sleep_backoff, utc_now


SYSTEM_PROMPT = """You are curating urban microbiome metadata for a scientific database.

Your job is to:
1. Decide whether the available metadata are sufficient to support a city-level curation decision.
2. Decide whether the sample truly appears to come from an urban or city-associated context.
3. Repair incorrect or missing annotations when the evidence supports doing so.

Rules:
- Use only the provided metadata and repository text. Do not invent facts.
- Prefer specific evidence from BioSample attributes, titles, descriptions, and run metadata.
- If the evidence is weak, mark the record as uncertain or insufficient rather than guessing.
- "Urban" includes city-associated built environment, transit, wastewater, sewage, municipal air, surfaces, and similar metropolitan sampling contexts.
- If a country, city, or assay class is clearly wrong or missing, provide corrected final values.
- If an original value already looks correct, keep it.
- Keep the response as compact JSON only.
"""


def _original_assay(rec: Dict[str, Any]) -> str:
    assay = rec.get("assay", {}) if isinstance(rec.get("assay", {}), dict) else {}
    return (assay.get("assay_class") or rec.get("runinfo_row", {}).get("LibraryStrategy") or "Unknown").strip()


def _original_country(rec: Dict[str, Any]) -> str:
    return ((rec.get("geo") or {}).get("country") or "").strip()


def _original_city(rec: Dict[str, Any]) -> str:
    return ((rec.get("geo") or {}).get("city") or "").strip()


def build_curation_context(
    rec: Dict[str, Any],
    biosample_cache: Dict[str, Any],
    bioproject_cache: Dict[str, Any],
) -> Tuple[Dict[str, Any], str]:
    runinfo = rec.get("runinfo_row", {}) if isinstance(rec.get("runinfo_row", {}), dict) else {}
    biosample_acc = (runinfo.get("BioSample") or (rec.get("geo") or {}).get("biosample_accession") or "").strip()
    bioproject_acc = (runinfo.get("BioProject") or (rec.get("bioproject") or {}).get("accession") or "").strip()
    biosample_details = biosample_cache.get(biosample_acc, {}) if biosample_acc else {}
    bioproject_details = bioproject_cache.get(bioproject_acc, {}) if bioproject_acc else {}

    context = {
        "srr": (rec.get("srr") or "").strip(),
        "sra_uid": (rec.get("sra_uid") or "").strip(),
        "title": (rec.get("title") or "").strip(),
        "original_annotations": {
            "country": _original_country(rec),
            "city": _original_city(rec),
            "assay_class": _original_assay(rec),
        },
        "runinfo": {
            "BioProject": bioproject_acc,
            "BioSample": biosample_acc,
            "CenterName": (runinfo.get("CenterName") or "").strip(),
            "Platform": (runinfo.get("Platform") or "").strip(),
            "Model": (runinfo.get("Model") or "").strip(),
            "LibraryStrategy": (runinfo.get("LibraryStrategy") or "").strip(),
            "LibrarySource": (runinfo.get("LibrarySource") or "").strip(),
            "LibrarySelection": (runinfo.get("LibrarySelection") or "").strip(),
            "ScientificName": (runinfo.get("ScientificName") or "").strip(),
            "ReleaseDate": (runinfo.get("ReleaseDate") or "").strip(),
        },
        "geo": rec.get("geo", {}) if isinstance(rec.get("geo", {}), dict) else {},
        "bioproject": {
            "accession": bioproject_acc,
            "title": (bioproject_details.get("title") or (rec.get("bioproject") or {}).get("title") or "").strip(),
            "description": (bioproject_details.get("description") or "").strip(),
            "name": (bioproject_details.get("name") or "").strip(),
            "data_type": (bioproject_details.get("data_type") or "").strip(),
        },
        "biosample": {
            "accession": biosample_acc,
            "title": (biosample_details.get("title") or "").strip(),
            "organism": (biosample_details.get("organism") or "").strip(),
            "attributes": biosample_details.get("attributes", {}) if isinstance(biosample_details.get("attributes", {}), dict) else {},
        },
    }
    signature = json.dumps(context, ensure_ascii=False, sort_keys=True)
    return context, signature


def _extract_output_text(payload: Dict[str, Any]) -> str:
    if isinstance(payload.get("output_text"), str) and payload["output_text"].strip():
        return payload["output_text"].strip()

    texts: List[str] = []
    for item in payload.get("output", []) or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content", []) or []:
            if not isinstance(content, dict):
                continue
            if isinstance(content.get("text"), str) and content["text"].strip():
                texts.append(content["text"].strip())
    if texts:
        return "\n".join(texts)
    raise RuntimeError("OpenAI response did not include parsable output text")


def _post_openai_json(body: Dict[str, Any], retries: int = 5) -> Dict[str, Any]:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured")

    req = urllib.request.Request(
        f"{OPENAI_BASE_URL}/responses",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {OPENAI_API_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    last_err: Optional[Exception] = None
    for i in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=180) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:  # pragma: no cover - network failure path
            last_err = exc
            _sleep_backoff(i)
    raise RuntimeError(f"OpenAI request failed: {last_err}")


def _normalize_result(rec: Dict[str, Any], raw: Dict[str, Any], model: str, signature: str) -> Dict[str, Any]:
    original_country = _original_country(rec)
    original_city = _original_city(rec)
    original_assay = _original_assay(rec)

    final_country = (raw.get("final_country") or original_country or "").strip()
    final_city = (raw.get("final_city") or original_city or "").strip()
    final_assay = (raw.get("final_assay_class") or original_assay or "Unknown").strip()

    change_country = _norm(final_country) != _norm(original_country) and bool(final_country)
    change_city = _norm(final_city) != _norm(original_city) and bool(final_city)
    change_assay = _norm(final_assay) != _norm(original_assay) and bool(final_assay)

    sufficiency = (raw.get("metadata_sufficiency") or "insufficient").strip().lower()
    if sufficiency not in {"sufficient", "partial", "insufficient"}:
        sufficiency = "partial"

    urban_origin = (raw.get("urban_origin") or "uncertain").strip().lower()
    if urban_origin not in {"urban", "non_urban", "uncertain"}:
        urban_origin = "uncertain"

    confidence = (raw.get("confidence") or "medium").strip().lower()
    if confidence not in {"low", "medium", "high"}:
        confidence = "medium"

    evidence = raw.get("evidence") if isinstance(raw.get("evidence"), list) else []
    evidence = [str(x).strip() for x in evidence if str(x).strip()][:8]

    return {
        "reviewed": True,
        "reviewed_utc": utc_now(),
        "model": model,
        "source_signature": signature,
        "metadata_sufficiency": sufficiency,
        "metadata_sufficient": sufficiency == "sufficient",
        "urban_origin": urban_origin,
        "confidence": confidence,
        "environment_type": (raw.get("environment_type") or "").strip(),
        "ai_fixed": change_country or change_city or change_assay,
        "original_country": original_country,
        "original_city": original_city,
        "original_assay_class": original_assay,
        "final_country": final_country,
        "final_city": final_city,
        "final_assay_class": final_assay,
        "change_country": change_country,
        "change_city": change_city,
        "change_assay_class": change_assay,
        "reasoning_summary": (raw.get("reasoning_summary") or "").strip(),
        "evidence": evidence,
    }


def curate_record(
    rec: Dict[str, Any],
    biosample_cache: Dict[str, Any],
    bioproject_cache: Dict[str, Any],
    model: str = OPENAI_MODEL,
) -> Tuple[Dict[str, Any], str]:
    context, signature = build_curation_context(rec, biosample_cache, bioproject_cache)
    body = {
        "model": model,
        "input": [
            {
                "role": "system",
                "content": [{"type": "input_text", "text": SYSTEM_PROMPT}],
            },
            {
                "role": "user",
                "content": [{
                    "type": "input_text",
                    "text": (
                        "Review this urban microbiome metadata record and return JSON with these keys only: "
                        "metadata_sufficiency, urban_origin, confidence, environment_type, "
                        "final_country, final_city, final_assay_class, reasoning_summary, evidence. "
                        "Use metadata_sufficiency in {sufficient, partial, insufficient}. "
                        "Use urban_origin in {urban, non_urban, uncertain}. "
                        "Use confidence in {low, medium, high}. "
                        "Evidence must be a short string array. "
                        f"\n\nRecord:\n{json.dumps(context, ensure_ascii=False, indent=2)}"
                    ),
                }],
            },
        ],
        "text": {
            "format": {
                "type": "json_object"
            }
        },
    }
    response = _post_openai_json(body)
    parsed = json.loads(_extract_output_text(response))
    return _normalize_result(rec, parsed, model, signature), signature


def curate_records(
    records: List[Dict[str, Any]],
    ai_cache: Dict[str, Any],
    biosample_cache: Dict[str, Any],
    bioproject_cache: Dict[str, Any],
    model: str = OPENAI_MODEL,
    overwrite: bool = False,
    max_records: int = 0,
) -> Dict[str, int]:
    counters = {"reviewed": 0, "skipped_cached": 0, "errors": 0}
    processed = 0

    for rec in records:
        srr = (rec.get("srr") or rec.get("runinfo_row", {}).get("Run") or "").strip()
        if not srr:
            continue
        if max_records and processed >= max_records:
            break

        context, signature = build_curation_context(rec, biosample_cache, bioproject_cache)
        existing = ai_cache.get(srr, {}) if isinstance(ai_cache.get(srr, {}), dict) else {}
        if existing and not overwrite and existing.get("source_signature") == signature and existing.get("reviewed"):
            counters["skipped_cached"] += 1
            continue

        try:
            # Reuse the context/signature computed above by passing through curate_record logic.
            body = {
                "model": model,
                "input": [
                    {
                        "role": "system",
                        "content": [{"type": "input_text", "text": SYSTEM_PROMPT}],
                    },
                    {
                        "role": "user",
                        "content": [{
                            "type": "input_text",
                            "text": (
                                "Review this urban microbiome metadata record and return JSON with these keys only: "
                                "metadata_sufficiency, urban_origin, confidence, environment_type, "
                                "final_country, final_city, final_assay_class, reasoning_summary, evidence. "
                                "Use metadata_sufficiency in {sufficient, partial, insufficient}. "
                                "Use urban_origin in {urban, non_urban, uncertain}. "
                                "Use confidence in {low, medium, high}. "
                                "Evidence must be a short string array. "
                                f"\n\nRecord:\n{json.dumps(context, ensure_ascii=False, indent=2)}"
                            ),
                        }],
                    },
                ],
                "text": {"format": {"type": "json_object"}},
            }
            response = _post_openai_json(body)
            parsed = json.loads(_extract_output_text(response))
            ai_cache[srr] = _normalize_result(rec, parsed, model, signature)
            counters["reviewed"] += 1
            processed += 1
        except Exception as exc:  # pragma: no cover - network/API failure path
            ai_cache[srr] = {
                "reviewed": False,
                "reviewed_utc": utc_now(),
                "model": model,
                "source_signature": signature,
                "error": str(exc),
            }
            counters["errors"] += 1
            processed += 1

    return counters
