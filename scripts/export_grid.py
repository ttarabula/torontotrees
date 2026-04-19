"""Bucket trees into ~1.1 km × 1.1 km cells for fast "near me" lookup.

Reads per-street JSON files already produced by export_site.py (which
have precomputed Street View headings), re-packages by grid cell.

Cell key: f"{ilat}_{ilon}" where
    ilat = floor((lat - 43.5) / 0.01)
    ilon = floor((lon + 79.7) / 0.014)

Output: site/data/near/<key>.json with {"trees": [...]}
"""
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parent.parent
STREET_DIR = ROOT / "site" / "data" / "street"
OUT_DIR = ROOT / "site" / "data" / "near"
OUT_DIR.mkdir(parents=True, exist_ok=True)

LAT0, LON0 = 43.5, -79.7
LAT_STEP = 0.01   # ~1.1 km at Toronto's latitude
LON_STEP = 0.014  # ~1.13 km after cos-correction


def cell_key(lat: float, lon: float) -> str:
    ilat = int((lat - LAT0) // LAT_STEP)
    ilon = int((lon - LON0) // LON_STEP)
    return f"{ilat}_{ilon}"


cells: dict[str, list[dict]] = {}
street_files = sorted(STREET_DIR.glob("*.json"))
print(f"reading {len(street_files)} per-street files…")
for sf in street_files:
    payload = json.loads(sf.read_text())
    street = payload["street"]
    for num, trees in payload["addresses"].items():
        for t in trees:
            lat, lon = t["lat"], t["lon"]
            rec = {
                "lat": lat, "lon": lon,
                "dbh_cm": t.get("dbh_cm"),
                "common": t.get("common"),
                "botanical": t.get("botanical"),
                "nbhd": t.get("nbhd"),
                "h": t.get("h"),
                "street": street, "num": num,
            }
            cells.setdefault(cell_key(lat, lon), []).append(rec)

total_trees = sum(len(v) for v in cells.values())
print(f"{total_trees:,} trees in {len(cells)} cells; writing…")
total_bytes = 0
for key, trees in cells.items():
    out = OUT_DIR / f"{key}.json"
    s = json.dumps({"trees": trees}, separators=(",", ":"))
    out.write_text(s)
    total_bytes += len(s)
print(f"{len(cells)} cells, {total_bytes/1e6:.1f} MB total")
