"""Export per-street JSON files for the address-lookup spike.

Layout:
  site/data/streets.json                — sorted list of all street names + counts
  site/data/street/<slug>.json          — trees on that street, grouped by house number

Slug: uppercase street name, spaces replaced with '_', non-[A-Z0-9_] stripped.
"""
from pathlib import Path
import json
import math
import re
import duckdb
import numpy as np
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

def compute_street_view_headings(group: pd.DataFrame, k: int = 10) -> np.ndarray:
    """Return a heading (0-360) per tree, pointing from the street toward the tree.

    Approach: fit a local street direction via PCA on each tree's k nearest
    neighbours on the same street (flat-projected to a local equirectangular
    frame). The Street View camera should face perpendicular to the street
    direction, on the side where the tree lives. We pick the perpendicular
    matching the sign of the tree's offset from the neighbour centroid.
    """
    lat = group["lat"].to_numpy()
    lon = group["lon"].to_numpy()
    n = len(lat)
    if n == 0:
        return np.zeros(0, dtype=int)
    lat_ref = float(np.mean(lat))
    cos_lat = math.cos(math.radians(lat_ref))
    # Flat-project: y = lat, x = lon * cos(lat_ref).  Units are degrees.
    xy = np.column_stack([lat, lon * cos_lat])

    k_eff = min(k, n)
    headings = np.zeros(n, dtype=int)
    if n == 1:
        return headings  # no direction inferable from one point
    for i in range(n):
        # k nearest neighbours to tree i, by Euclidean distance in flat coords
        d2 = np.sum((xy - xy[i]) ** 2, axis=1)
        nbr_idx = np.argpartition(d2, k_eff - 1)[:k_eff]
        nbrs = xy[nbr_idx]
        # PCA primary direction of the neighbours
        centered = nbrs - nbrs.mean(axis=0)
        cov = centered.T @ centered
        eigvals, eigvecs = np.linalg.eigh(cov)
        street_axis = eigvecs[:, -1]                     # (dy, dx)
        perp_axis = np.array([-street_axis[1], street_axis[0]])
        offset = xy[i] - nbrs.mean(axis=0)
        sign = 1.0 if float(offset @ perp_axis) >= 0 else -1.0
        direction = perp_axis * sign
        # Compass bearing clockwise from north: atan2(east, north)
        bearing = math.degrees(math.atan2(float(direction[1]), float(direction[0]))) % 360
        headings[i] = int(round(bearing)) % 360
    return headings


written = 0
total_bytes = 0
for street, g in df.groupby("street", sort=False):
    g = g.reset_index(drop=True)
    headings = compute_street_view_headings(g)
    by_addr: dict[str, list[dict]] = {}
    for i, r in enumerate(g.itertuples(index=False)):
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
            "h": int(headings[i]),
        })
    out = STREET_DIR / f"{slug(street)}.json"
    payload = {"street": street, "addresses": by_addr}
    s = json.dumps(payload, separators=(",", ":"))
    out.write_text(s)
    total_bytes += len(s)
    written += 1

print(f"  {written:,} files, {total_bytes/1e6:.1f} MB total")
