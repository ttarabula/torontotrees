"""Export per-street JSON files for the address-lookup spike.

Layout:
  site/data/streets.json                — sorted list of all street names + counts
  site/data/street/<slug>.json          — trees on that street, grouped by house number

Slug: uppercase street name, spaces replaced with '_', non-[A-Z0-9_] stripped.
"""
from pathlib import Path
import json
import re
import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
TREES = ROOT / "data" / "processed" / "trees.parquet"
SITE = ROOT / "site"
DATA = SITE / "data"
STREET_DIR = DATA / "street"
DATA.mkdir(parents=True, exist_ok=True)
STREET_DIR.mkdir(parents=True, exist_ok=True)


def slug(name: str) -> str:
    s = re.sub(r"[^A-Z0-9]+", "_", name.upper()).strip("_")
    return s or "UNKNOWN"


con = duckdb.connect()
con.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{TREES}')")

# Street index
print("building street index…")
streets = con.execute("""
    SELECT STREETNAME AS street, COUNT(*) AS n
    FROM t WHERE STREETNAME IS NOT NULL
    GROUP BY 1 ORDER BY 1
""").fetchdf()
streets["slug"] = streets["street"].map(slug)
index = [
    {"street": r.street, "slug": r.slug, "n": int(r.n)}
    for r in streets.itertuples(index=False)
]
with (DATA / "streets.json").open("w") as f:
    json.dump(index, f)
print(f"  {len(index):,} streets -> streets.json ({(DATA / 'streets.json').stat().st_size/1024:.1f} KB)")

# Per-street: group trees by house number
print("writing per-street files…")
df = con.execute("""
    SELECT STREETNAME AS street,
           ADDRESS    AS num,
           SUFFIX     AS suffix,
           common_raw AS common,
           botanical_raw AS botanical,
           dbh_cm,
           lat, lon,
           nbhd_name
    FROM t WHERE STREETNAME IS NOT NULL
""").fetchdf()

written = 0
total_bytes = 0
for street, g in df.groupby("street", sort=False):
    by_addr: dict[str, list[dict]] = {}
    for r in g.itertuples(index=False):
        sfx = r.suffix
        if pd.isna(sfx) or str(sfx).strip() in {"", "None", "NONE"}:
            suffix = ""
        else:
            suffix = str(sfx).strip()
        key = f"{int(r.num)}{suffix}"
        by_addr.setdefault(key, []).append({
            "common": r.common,
            "botanical": r.botanical,
            "dbh_cm": None if pd.isna(r.dbh_cm) else int(r.dbh_cm),
            "lat": round(float(r.lat), 6),
            "lon": round(float(r.lon), 6),
            "nbhd": r.nbhd_name,
        })
    out = STREET_DIR / f"{slug(street)}.json"
    payload = {"street": street, "addresses": by_addr}
    s = json.dumps(payload, separators=(",", ":"))
    out.write_text(s)
    total_bytes += len(s)
    written += 1

print(f"  {written:,} files, {total_bytes/1e6:.1f} MB total")
