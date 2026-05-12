#!/usr/bin/env python3
"""Comprehensive Comic Vine data fetch for Pete's Corner.

Pulls:
  - Cover URLs (Phase 1)
  - Per-issue: synopsis, character credits, creator credits, story arc credits, cover date (Phase 1)
  - Per-character: first-appearance issue, image, deck, movie/TV credits (Phase 2)
  - Volume totals for run-completeness math (Phase 1)

Writes:
  - comics.csv with new "Cover URL" column
  - data/metadata.json   — { "<volumeId>:<issueNum>": {...} }
  - data/characters.json — { "<charId>": { name, image, firstAppearance, movies } }
  - data/volumes.json    — { "<volumeId>": { name, totalIssues, startYear } }

Idempotent — caches every API result in scripts/.cv_cache.json so you can stop
and resume freely.

Usage:
  export COMICVINE_API_KEY=...
  python3 scripts/fetch_data.py
"""
import csv
import json
import os
import re
import sys
import time
from pathlib import Path

try:
    import requests
except ImportError:
    print("Install requests first: pip install requests", file=sys.stderr); sys.exit(1)

ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = ROOT / "comics.csv"
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)
CACHE_PATH = ROOT / "scripts" / ".cv_cache.json"
META_PATH = DATA_DIR / "metadata.json"
CHARS_PATH = DATA_DIR / "characters.json"
VOLS_PATH = DATA_DIR / "volumes.json"

API_BASE = "https://comicvine.gamespot.com/api"
HEADERS = {"User-Agent": "petes-corner/1.0"}
THROTTLE = 1.05
API_KEY = os.environ.get("COMICVINE_API_KEY", "").strip()
if not API_KEY:
    print("ERROR: set COMICVINE_API_KEY env var first", file=sys.stderr); sys.exit(1)


def load_cache():
    if CACHE_PATH.exists():
        try: return json.loads(CACHE_PATH.read_text())
        except Exception: return {}
    return {}


def save_cache(cache):
    CACHE_PATH.write_text(json.dumps(cache))


_last = 0.0
def cv_get(path, **params):
    global _last
    el = time.time() - _last
    if el < THROTTLE: time.sleep(THROTTLE - el)
    params["api_key"] = API_KEY; params["format"] = "json"
    backoff = 5
    for _ in range(6):
        try:
            r = requests.get(f"{API_BASE}{path}", params=params, headers=HEADERS, timeout=30)
            _last = time.time()
            if r.status_code == 429:
                print(f"    rate-limited, sleeping {backoff}s…", flush=True)
                time.sleep(backoff); backoff *= 2; continue
            r.raise_for_status()
            d = r.json()
            if d.get("status_code") != 1:
                err = d.get("error", "")
                if "limit" in err.lower():
                    print(f"    CV rate limit ({err}), sleeping {backoff}s…", flush=True)
                    time.sleep(backoff); backoff *= 2; continue
                return None
            return d
        except requests.RequestException as e:
            print(f"    network error ({e}); retry in {backoff}s…", flush=True)
            time.sleep(backoff); backoff *= 2
    return None


def clean_series(name):
    n = re.sub(r",\s*Vol\.?\s*\d+\s*$", "", name, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", n).strip()


def find_volume(series_name, cache):
    key = f"vol:{series_name}"
    if key in cache: return cache[key]
    cleaned = clean_series(series_name)
    d = cv_get("/volumes/", filter=f"name:{cleaned}",
               field_list="id,name,start_year,publisher,count_of_issues", limit=10)
    if not d or not d.get("results"):
        cache[key] = None; return None
    res = d["results"]
    exact = [v for v in res if v.get("name", "").lower() == cleaned.lower()]
    pick = exact[0] if exact else res[0]
    cache[key] = {
        "id": pick.get("id"),
        "name": pick.get("name"),
        "totalIssues": pick.get("count_of_issues"),
        "startYear": pick.get("start_year"),
        "publisher": (pick.get("publisher") or {}).get("name"),
    }
    return cache[key]


def fetch_volume_issues(volume_id, cache):
    """Return dict { issueNumStr: rich issue dict } for a volume."""
    key = f"vissues:{volume_id}"
    if key in cache: return cache[key]
    issues = {}
    offset = 0
    while True:
        d = cv_get("/issues/",
                   filter=f"volume:{volume_id}",
                   field_list="issue_number,name,deck,image,character_credits,person_credits,story_arc_credits,cover_date",
                   offset=offset, limit=100)
        if not d: break
        for it in d.get("results", []):
            num = (it.get("issue_number") or "").strip()
            if not num: continue
            img = it.get("image") or {}
            cover = img.get("medium_url") or img.get("original_url") or img.get("small_url")
            issues[num] = {
                "name": it.get("name") or "",
                "synopsis": it.get("deck") or "",
                "cover": cover or "",
                "coverDate": it.get("cover_date") or "",
                "characters": [{"id": c["id"], "name": c.get("name")} for c in (it.get("character_credits") or [])][:30],
                "creators": [{"id": p["id"], "name": p.get("name"), "role": p.get("role")} for p in (it.get("person_credits") or [])][:15],
                "arcs": [{"id": a["id"], "name": a.get("name")} for a in (it.get("story_arc_credits") or [])],
            }
        total = d.get("number_of_total_results", 0)
        got = d.get("number_of_page_results", 0) or len(d.get("results", []))
        offset += got
        if offset >= total or got == 0: break
    cache[key] = issues
    return issues


def fetch_character_detail(char_id, cache):
    key = f"char:{char_id}"
    if key in cache: return cache[key]
    d = cv_get(f"/character/4005-{char_id}/",
               field_list="id,name,image,deck,first_appeared_in_issue,movies,publisher,gender,real_name,count_of_issue_appearances")
    if not d or not d.get("results"):
        cache[key] = None; return None
    r = d["results"]
    img = r.get("image") or {}
    fa = r.get("first_appeared_in_issue") or {}
    cache[key] = {
        "name": r.get("name") or "",
        "realName": r.get("real_name") or "",
        "image": img.get("medium_url") or img.get("small_url") or "",
        "deck": r.get("deck") or "",
        "publisher": (r.get("publisher") or {}).get("name") or "",
        "appearances": r.get("count_of_issue_appearances") or 0,
        "firstAppearance": {
            "volumeId": (fa.get("volume") or {}).get("id") if isinstance(fa.get("volume"), dict) else None,
            "issueNum": (fa.get("issue_number") or "").strip() if fa else "",
            "issueId": fa.get("id") if fa else None,
        } if fa else None,
        "movies": [{"id": m["id"], "name": m.get("name")} for m in (r.get("movies") or [])],
    }
    return cache[key]


def main():
    if not CSV_PATH.exists():
        print(f"missing {CSV_PATH}", file=sys.stderr); sys.exit(1)
    rows = list(csv.DictReader(open(CSV_PATH, newline="", encoding="utf-8")))
    print(f"loaded {len(rows)} rows from comics.csv")

    cache = load_cache()
    series_names = sorted({r["Series"] for r in rows if r.get("Series")})
    print(f"{len(series_names)} unique series\n")

    # ==================== PHASE 1: Volumes + Issues ====================
    print("=" * 60)
    print("PHASE 1: volumes + issues (covers, synopsis, characters)")
    print("=" * 60)
    volumes_data = {}
    for i, name in enumerate(series_names, 1):
        v = find_volume(name, cache)
        if v and v.get("id"):
            volumes_data[name] = v
            print(f"[{i}/{len(series_names)}] ✓ {name} → vol {v['id']} ({v.get('totalIssues') or '?'} issues)", flush=True)
            fetch_volume_issues(v["id"], cache)
        else:
            print(f"[{i}/{len(series_names)}] ✗ {name} (no match)", flush=True)
        if i % 25 == 0:
            save_cache(cache)
            print(f"  -- cache saved at series {i}", flush=True)
    save_cache(cache)

    # Build per-row enrichment
    print("\nbuilding per-row enrichment…")
    metadata = {}      # key "series|issue" — directly matches CSV rows
    series_to_volume = {}
    all_char_ids = set()
    for row in rows:
        v = volumes_data.get(row["Series"])
        if not v: continue
        series_to_volume[row["Series"]] = v["id"]
        vissues = cache.get(f"vissues:{v['id']}") or {}
        num_raw = (row.get("Issue") or "").strip()
        num_norm = re.sub(r"[^0-9.]", "", num_raw)
        match = vissues.get(num_raw) or vissues.get(num_norm)
        if match:
            enriched = dict(match)
            enriched["volumeId"] = v["id"]
            enriched["series"] = row["Series"]
            metadata[f"{row['Series']}|{num_raw}"] = enriched
            for c in match.get("characters", []):
                all_char_ids.add(c["id"])

    # Volumes summary (for completeness)
    volumes_summary = {}
    for series, v in volumes_data.items():
        if not v.get("id"): continue
        volumes_summary[series] = {
            "id": v.get("id"),
            "name": v.get("name"),
            "totalIssues": v.get("totalIssues"),
            "startYear": v.get("startYear"),
            "publisher": v.get("publisher"),
        }

    VOLS_PATH.write_text(json.dumps(volumes_summary, indent=0))
    META_PATH.write_text(json.dumps(metadata, indent=0))
    print(f"\nwrote {VOLS_PATH.name} ({len(volumes_summary)} volumes), {META_PATH.name} ({len(metadata)} issues with rich data)")

    # ==================== PHASE 1.5: Update CSV with Cover URL ====================
    print("\nupdating comics.csv with Cover URL column…")
    fieldnames = list(rows[0].keys())
    if "Cover URL" not in fieldnames:
        fieldnames.append("Cover URL")
    out_rows = []
    matched = 0
    for row in rows:
        v = volumes_data.get(row["Series"])
        url = ""
        if v and v.get("id"):
            vissues = cache.get(f"vissues:{v['id']}") or {}
            num_raw = (row.get("Issue") or "").strip()
            num_norm = re.sub(r"[^0-9.]", "", num_raw)
            match = vissues.get(num_raw) or vissues.get(num_norm)
            if match: url = match.get("cover") or ""
        if url: matched += 1
        row["Cover URL"] = url
        out_rows.append(row)
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        w.writeheader(); w.writerows(out_rows)
    print(f"  matched covers for {matched}/{len(out_rows)} comics")

    # ==================== PHASE 2: Character details ====================
    print()
    print("=" * 60)
    print(f"PHASE 2: character details ({len(all_char_ids)} unique chars)")
    print("=" * 60)
    characters_out = {}
    for i, cid in enumerate(sorted(all_char_ids), 1):
        c = fetch_character_detail(cid, cache)
        if c: characters_out[str(cid)] = c
        if i % 25 == 0:
            print(f"[{i}/{len(all_char_ids)}] processed", flush=True)
            save_cache(cache)
            CHARS_PATH.write_text(json.dumps(characters_out, indent=0))
    save_cache(cache)
    CHARS_PATH.write_text(json.dumps(characters_out, indent=0))
    print(f"\nwrote {CHARS_PATH.name} ({len(characters_out)} characters)")

    # ==================== SUMMARY ====================
    fa_owned = 0
    movie_chars = 0
    # Build reverse index: volumeId+issueNumNorm → series+issueRaw for FA matching
    owned_by_vol_issue = {}
    for row in rows:
        v = volumes_data.get(row["Series"])
        if not v or not v.get("id"): continue
        num_raw = (row.get("Issue") or "").strip()
        num_norm = re.sub(r"[^0-9.]", "", num_raw)
        owned_by_vol_issue[(v["id"], num_norm or num_raw)] = (row["Series"], num_raw)
    for cid, ch in characters_out.items():
        if ch.get("movies"): movie_chars += 1
        fa = ch.get("firstAppearance")
        if fa and fa.get("volumeId") and fa.get("issueNum"):
            num_norm = re.sub(r"[^0-9.]", "", fa["issueNum"])
            if (fa["volumeId"], num_norm) in owned_by_vol_issue: fa_owned += 1
    print(f"\n🔥 First appearances owned in collection: {fa_owned}")
    print(f"🎬 Characters with movie/TV credits: {movie_chars}")
    print(f"\n✓ Done. UI will pick up new files automatically on page load.")


if __name__ == "__main__":
    main()
