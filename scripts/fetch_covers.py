#!/usr/bin/env python3
"""Fetch cover image URLs from Comic Vine for every comic in comics.csv.

Usage:
  export COMICVINE_API_KEY=your_key_here   # get one at https://comicvine.gamespot.com/api/
  python3 scripts/fetch_covers.py

The script is idempotent — it caches every API result in scripts/.cv_cache.json
so you can stop/resume freely. Re-running is cheap once warm.

Strategy:
  - For each unique series name in comics.csv, find its Comic Vine volume ID.
  - For each volume, fetch all issues (paginated). Cache image URLs by issue_number.
  - Match each row of comics.csv to (volume, issue_number) and write cover_url.
  - Throttle to 1 req/sec to stay within Comic Vine's rate limits.

Output: writes comics.csv with a new 'Cover URL' column appended.
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
    print("Install requests first: pip install requests", file=sys.stderr)
    sys.exit(1)

ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = ROOT / "comics.csv"
CACHE_PATH = ROOT / "scripts" / ".cv_cache.json"
API_BASE = "https://comicvine.gamespot.com/api"
HEADERS = {"User-Agent": "petes-corner-cover-fetch/1.0"}
THROTTLE = 1.05  # seconds between requests

API_KEY = os.environ.get("COMICVINE_API_KEY", "").strip()
if not API_KEY:
    print("ERROR: set COMICVINE_API_KEY env var first.", file=sys.stderr)
    print("Get a free key at https://comicvine.gamespot.com/api/", file=sys.stderr)
    sys.exit(1)


def load_cache():
    if CACHE_PATH.exists():
        try:
            return json.loads(CACHE_PATH.read_text())
        except Exception:
            return {}
    return {}


def save_cache(cache):
    CACHE_PATH.write_text(json.dumps(cache, indent=2))


_last_req = 0.0


def cv_get(path, **params):
    """Throttled GET against Comic Vine API."""
    global _last_req
    elapsed = time.time() - _last_req
    if elapsed < THROTTLE:
        time.sleep(THROTTLE - elapsed)
    params["api_key"] = API_KEY
    params["format"] = "json"
    url = f"{API_BASE}{path}"
    backoff = 4
    for attempt in range(5):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=30)
            _last_req = time.time()
            if r.status_code == 429:
                print(f"  rate-limited, sleeping {backoff}s…", flush=True)
                time.sleep(backoff)
                backoff *= 2
                continue
            r.raise_for_status()
            data = r.json()
            if data.get("status_code") != 1:
                # Comic Vine returns 200 with status_code != 1 on errors
                err = data.get("error", "unknown")
                if "limit" in err.lower():
                    print(f"  CV rate limit hit ({err}), sleeping {backoff}s…", flush=True)
                    time.sleep(backoff)
                    backoff *= 2
                    continue
                return None
            return data
        except requests.RequestException as e:
            print(f"  request error ({e}); retrying in {backoff}s…", flush=True)
            time.sleep(backoff)
            backoff *= 2
    return None


def clean_series(name):
    """Normalize for searching — strip volume suffix, trailing punctuation."""
    n = re.sub(r",\s*Vol\.?\s*\d+\s*$", "", name, flags=re.IGNORECASE)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def find_volume_id(series_name, cache):
    """Find the Comic Vine volume id for a series name."""
    key = f"volume:{series_name}"
    if key in cache:
        return cache[key]
    cleaned = clean_series(series_name)
    print(f"  searching volume: {cleaned!r}", flush=True)
    data = cv_get("/volumes/", filter=f"name:{cleaned}", field_list="id,name,start_year,publisher,count_of_issues", limit=10)
    if not data or not data.get("results"):
        cache[key] = None
        return None
    # Prefer exact (case-insensitive) name match
    results = data["results"]
    exact = [v for v in results if v.get("name", "").lower() == cleaned.lower()]
    pick = exact[0] if exact else results[0]
    cache[key] = pick.get("id")
    return cache[key]


def fetch_issues_for_volume(volume_id, cache):
    """Return dict { issue_number_str: cover_url } for a volume."""
    key = f"issues:{volume_id}"
    if key in cache:
        return cache[key]
    issues = {}
    offset = 0
    while True:
        data = cv_get(
            "/issues/",
            filter=f"volume:{volume_id}",
            field_list="issue_number,image",
            offset=offset,
            limit=100,
        )
        if not data:
            break
        for issue in data.get("results", []):
            num = (issue.get("issue_number") or "").strip()
            if not num:
                continue
            img = issue.get("image") or {}
            url = img.get("medium_url") or img.get("original_url") or img.get("small_url")
            if url:
                issues[num] = url
        total = data.get("number_of_total_results", 0)
        offset += data.get("number_of_page_results", 0) or len(data.get("results", []))
        if offset >= total or not data.get("results"):
            break
    cache[key] = issues
    return issues


def normalize_issue(issue_str):
    """Comic Vine stores issue_number as plain digits (e.g., '361').
    Our CSV has variants like '361B', '1A'. Try a few mappings."""
    base = re.sub(r"[^0-9.]", "", issue_str or "")
    return base


def main():
    rows = list(csv.DictReader(open(CSV_PATH, newline="", encoding="utf-8")))
    if not rows:
        print("No rows in comics.csv", file=sys.stderr)
        sys.exit(1)
    print(f"Loaded {len(rows)} comics from CSV")

    cache = load_cache()
    # Group by series name
    series_names = sorted(set(r["Series"] for r in rows))
    print(f"{len(series_names)} unique series to look up\n")

    # Look up volumes
    for i, name in enumerate(series_names, 1):
        print(f"[{i}/{len(series_names)}] {name}")
        vid = find_volume_id(name, cache)
        if vid:
            fetch_issues_for_volume(vid, cache)
        else:
            print(f"  no match for {name!r}")
        if i % 20 == 0:
            save_cache(cache)
            print(f"  -- cache saved ({i}/{len(series_names)})")

    save_cache(cache)

    # Now build cover_url for each row
    print("\nMatching rows to cover URLs…")
    out_rows = []
    fieldnames = list(rows[0].keys())
    if "Cover URL" not in fieldnames:
        fieldnames.append("Cover URL")
    matched = 0
    for row in rows:
        vid = cache.get(f"volume:{row['Series']}")
        url = ""
        if vid:
            issues = cache.get(f"issues:{vid}") or {}
            num = normalize_issue(row["Issue"])
            url = issues.get(num) or issues.get(row["Issue"]) or ""
        if url:
            matched += 1
        row["Cover URL"] = url
        out_rows.append(row)

    # Write back
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        w.writerows(out_rows)

    print(f"\nDone. Matched cover URLs for {matched}/{len(out_rows)} comics.")
    print(f"Updated CSV: {CSV_PATH}")
    print(f"Cache (re-run friendly): {CACHE_PATH}")


if __name__ == "__main__":
    main()
