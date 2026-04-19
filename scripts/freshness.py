"""Fetch the CKAN metadata for the Street Tree dataset and write a tiny
freshness file the homepage footer can read.

Output: site/data/freshness.json containing
  { "source_last_refreshed": "2026-02-20",
    "site_built": "2026-04-19" }

Any failure falls back to "unknown" for the upstream date so the build
never breaks on an API hiccup.
"""
from pathlib import Path
import datetime as dt
import json

import requests

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "site" / "data" / "freshness.json"

PKG_URL = "https://ckan0.cf.opendata.inter.prod-toronto.ca/api/3/action/package_show?id=street-tree-data"


def fetch_upstream_date() -> str | None:
    try:
        r = requests.get(PKG_URL, timeout=20)
        r.raise_for_status()
        pkg = r.json().get("result", {})
        # Prefer the explicit last_refreshed field when present, otherwise
        # fall back to metadata_modified. Both are ISO strings.
        for key in ("last_refreshed", "metadata_modified"):
            val = pkg.get(key)
            if val:
                return val[:10]  # YYYY-MM-DD
    except Exception as e:
        print(f"warning: couldn't fetch CKAN metadata — {e}")
    return None


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "source_last_refreshed": fetch_upstream_date() or "unknown",
        "site_built": dt.date.today().isoformat(),
    }
    OUT.write_text(json.dumps(data, indent=2) + "\n")
    print(f"wrote {OUT.relative_to(ROOT)}: {data}")


if __name__ == "__main__":
    main()
