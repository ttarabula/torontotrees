"""Estimate planting year from DBH × species growth rate. Export a compact
binary for the timeline scrubber page.

Tree age estimation is rough — typical urban-tree growth varies roughly
±40% depending on site quality, species, and care. But at 685K-tree
city scale, the broad strokes (young plantings vs. veteran trees) are
visible even through the noise.

Output: site/data/timeline.bin (binary, ~3.4 MB uncompressed, ~1.5 MB gzipped).
Format:
  [header]
    u8  version = 1
    u8  num_decades
    u16 width_px
    u16 height_px
    f32 lat_min, lat_max, lon_min, lon_max
    u16 earliest_year (= first decade's start, e.g. 1900)
    u16 latest_year   (= last decade's end,   e.g. 2030)
  Then one block per tree, sorted by estimated year ascending:
    u16 year_offset  (from earliest_year)
    u16 x_px
    u16 y_px

Canvas renders dots. Slider filters by year (cumulative).
"""
from pathlib import Path
import struct
import duckdb

ROOT = Path(__file__).resolve().parent.parent
PROC = ROOT / "data" / "processed"
OUT = ROOT / "site" / "data" / "timeline.bin"

# Canvas dimensions. Toronto is ~30km east-west × ~20km north-south.
# A 1400×900 canvas gives ~21m/px — comfortably wider than the ~8m
# spacing between adjacent street trees, so dots don't overlap.
WIDTH = 1400
HEIGHT = 900

# Toronto bbox (tight to the populated area).
LAT_MIN, LAT_MAX = 43.58, 43.86
LON_MIN, LON_MAX = -79.64, -79.12

CURRENT_YEAR = 2026
MIN_YEAR = 1900   # floor for "veteran" trees
DEFAULT_GROWTH = 0.75   # cm DBH per year — typical urban street tree

# Botanical-key prefix → cm DBH / year. Trees growing fast in urban settings
# (silver maple, Freeman hybrids, poplar, London plane) vs. slow (oak, beech,
# ginkgo). Default = 0.75 cm/yr for species not listed.
GROWTH_RATES: dict[str, float] = {
    # Fast — soft-wooded / hybrid vigour
    "acer saccharinum":    1.10,
    "acer freemanii":      1.05,
    "acer x freemanii":    1.05,
    "populus":             1.20,
    "salix":               1.20,
    "platanus":            1.00,
    "liriodendron":        0.95,
    "catalpa":             0.90,
    "robinia":             0.90,
    "ailanthus":           1.15,

    # Slow — hardwood, long-lived
    "quercus":             0.55,
    "fagus":               0.50,
    "ginkgo":              0.50,
    "celtis":              0.60,
    "carpinus":            0.50,
    "ostrya":              0.45,
    "aesculus":            0.60,
    "carya":               0.55,
    "taxodium":            0.60,
    "cercidiphyllum":      0.55,
    "cladrastis":          0.55,
}


def growth_rate(botanical_key: str | None) -> float:
    if not botanical_key:
        return DEFAULT_GROWTH
    # Longest-prefix match (so "quercus rubra" hits "quercus")
    for prefix in sorted(GROWTH_RATES, key=len, reverse=True):
        if botanical_key.startswith(prefix):
            return GROWTH_RATES[prefix]
    return DEFAULT_GROWTH


def lat_to_y(lat: float) -> int:
    # Flip: larger lat = top of canvas (smaller y)
    return int(round(HEIGHT * (LAT_MAX - lat) / (LAT_MAX - LAT_MIN)))


def lon_to_x(lon: float) -> int:
    return int(round(WIDTH * (lon - LON_MIN) / (LON_MAX - LON_MIN)))


def main():
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT botanical_key, dbh_cm, lat, lon
        FROM read_parquet('{PROC}/trees.parquet')
        WHERE lat BETWEEN {LAT_MIN} AND {LAT_MAX}
          AND lon BETWEEN {LON_MIN} AND {LON_MAX}
    """).fetchall()
    print(f"loaded {len(rows):,} trees in bbox")

    records = []
    unknown_dbh = 0
    for botanical_key, dbh_cm, lat, lon in rows:
        if dbh_cm is None or dbh_cm <= 0 or dbh_cm > 200:
            # Trees with no recorded DBH are almost always new plantings the
            # city hasn't measured yet — assign them to the current year.
            unknown_dbh += 1
            year = CURRENT_YEAR
        else:
            rate = growth_rate(botanical_key)
            age = dbh_cm / rate
            year = int(round(CURRENT_YEAR - age))
            if year < MIN_YEAR:
                year = MIN_YEAR
            if year > CURRENT_YEAR:
                year = CURRENT_YEAR
        x = lon_to_x(lon)
        y = lat_to_y(lat)
        if x < 0 or x >= WIDTH or y < 0 or y >= HEIGHT:
            continue
        records.append((year, x, y))

    records.sort(key=lambda r: r[0])

    # Quick stats
    print(f"records encoded: {len(records):,}")
    print(f"trees with no DBH (assumed recent plantings): {unknown_dbh:,}")
    from collections import Counter
    buckets = Counter((r[0] // 10) * 10 for r in records)
    for decade in sorted(buckets):
        bar = "#" * int(buckets[decade] / max(buckets.values()) * 40)
        print(f"  {decade}s: {buckets[decade]:>7,}  {bar}")

    # Encode
    OUT.parent.mkdir(parents=True, exist_ok=True)
    earliest = MIN_YEAR
    latest = CURRENT_YEAR
    # Header: version, num_decades, w, h, lat_min/max/lon_min/max, earliest, latest
    num_decades = (latest - earliest) // 10 + 1
    header = struct.pack(
        "<BBHHffffHH",
        1, num_decades, WIDTH, HEIGHT,
        LAT_MIN, LAT_MAX, LON_MIN, LON_MAX,
        earliest, latest,
    )
    chunks = [header]
    for year, x, y in records:
        chunks.append(struct.pack("<HHH", year - earliest, x, y))
    blob = b"".join(chunks)
    OUT.write_bytes(blob)
    print(f"wrote {OUT.relative_to(ROOT)} — {len(blob):,} bytes ({len(records):,} trees)")


if __name__ == "__main__":
    main()
