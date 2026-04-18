"""Fetch Wikipedia summaries (thumbnail + extract + page URL) for every tree species.

Writes:
  site/data/species.json   { "genus species" -> { wiki_url, thumb, extract, title, display_common } }

Keys are lowercased "genus species" (first two tokens of botanical name).
We pick the most-common common_raw per key for display fallback.
"""
from pathlib import Path
import json
import re
import time
import duckdb
import requests

ROOT = Path(__file__).resolve().parent.parent
TREES = ROOT / "data" / "processed" / "trees.parquet"
OUT = ROOT / "site" / "data" / "species.json"
CACHE = ROOT / "data" / "processed" / "_wiki_cache.json"

UA = "torontotrees/0.1 (https://github.com/ttarabula/torontotrees) open-data civic project"
API = "https://en.wikipedia.org/api/rest_v1/page/summary/{title}"


def genus_species(botanical_raw: str) -> str | None:
    if not botanical_raw:
        return None
    s = botanical_raw.lower().strip()
    # strip cultivar ('x'), f., var., subsp., and quoted cultivar names
    s = re.sub(r"'[^']*'", "", s)
    s = re.sub(r"\b(f|var|subsp|ssp|cv|x)\.?\b", " ", s)
    s = re.sub(r"[^a-z\- ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    parts = s.split(" ")
    if len(parts) < 2:
        return parts[0] if parts else None
    return f"{parts[0]} {parts[1]}"


def fetch_one(title: str) -> dict | None:
    url = API.format(title=requests.utils.quote(title.replace(" ", "_"), safe=""))
    r = requests.get(url, headers={"User-Agent": UA}, timeout=15)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    j = r.json()
    if j.get("type") == "disambiguation":
        return None
    return {
        "title": j.get("title"),
        "extract": (j.get("extract") or "")[:400],
        "wiki_url": (j.get("content_urls") or {}).get("desktop", {}).get("page"),
        "thumb": (j.get("thumbnail") or {}).get("source"),
    }


def main() -> None:
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT botanical_raw, common_raw, COUNT(*) AS n
        FROM read_parquet('{TREES}')
        WHERE botanical_raw IS NOT NULL
        GROUP BY 1, 2
    """).fetchdf()

    # keyed by (genus species) — pick most-common common_raw for display fallback
    grouped: dict[str, dict] = {}
    for r in rows.itertuples(index=False):
        key = genus_species(r.botanical_raw)
        if not key:
            continue
        g = grouped.setdefault(key, {"count": 0, "common_votes": {}, "bot_variants": set()})
        g["count"] += r.n
        g["common_votes"][r.common_raw] = g["common_votes"].get(r.common_raw, 0) + r.n
        g["bot_variants"].add(r.botanical_raw)

    print(f"{len(grouped)} distinct genus+species")

    cache: dict[str, dict] = {}
    if CACHE.exists():
        cache = json.loads(CACHE.read_text())
        print(f"  cache hit candidates: {len(cache)}")

    out: dict[str, dict] = {}
    fetched = 0
    for key in sorted(grouped):
        meta = grouped[key]
        common = max(meta["common_votes"].items(), key=lambda kv: kv[1])[0]
        entry = {
            "count": meta["count"],
            "display_common": common,
        }
        if key in cache:
            entry.update(cache[key] or {})
        else:
            # Prefer the "Genus species" botanical title for Wikipedia
            title = key[:1].upper() + key[1:]
            wiki = fetch_one(title)
            if not wiki and common:
                # fallback to common name
                wiki = fetch_one(common.split(",")[0].strip())
            cache[key] = wiki or {}
            if wiki:
                entry.update(wiki)
            fetched += 1
            time.sleep(0.1)  # be polite
        out[key] = entry

    if fetched:
        CACHE.write_text(json.dumps(cache, indent=2))
    OUT.write_text(json.dumps(out, separators=(",", ":")))
    with_thumb = sum(1 for v in out.values() if v.get("thumb"))
    print(f"wrote {OUT} — {with_thumb}/{len(out)} have thumbnails, fetched {fetched} now")


if __name__ == "__main__":
    main()
