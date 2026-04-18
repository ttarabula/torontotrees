"""Aggregate the 2008 land-cover shapefile by neighbourhood, to match 2018 format.

2008 source: 6.5M polygons, 2 fields (OBJECTID, gridcode), in NAD27/MTM zone 10.
Output: one row per (neighbourhood, gridcode) with total area in m².
"""
from pathlib import Path
import duckdb

ROOT = Path(__file__).resolve().parent.parent
SHP = ROOT / "data" / "raw" / "landcover2008" / "cotlandcover" / "cotLandcover.shp"
NBHD = ROOT / "data" / "raw" / "neighbourhoods-4326.geojson"
OUT = ROOT / "data" / "processed" / "nbhd_canopy_2008.parquet"

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")

print("Loading 2008 landcover (6.5M polygons)...")
# Compute area in native projection; reproject centroid for spatial join.
# Exclude polygons > 10 km² — there's one ~56 km² "road" polygon that's clearly
# a data artifact (Toronto's entire road network assigned as one shape).
con.execute(f"""
    CREATE TABLE lc2008 AS
    SELECT gridcode,
           ST_Area(geom) AS area_m2,
           ST_Transform(ST_Centroid(geom), 'EPSG:2019', 'EPSG:4326', always_xy := TRUE) AS centroid_wgs84
    FROM ST_Read('{SHP}')
    WHERE ST_Area(geom) <= 1e7
""")
excluded = con.execute(f"""
    SELECT COUNT(*) AS n, SUM(ST_Area(geom)) AS area
    FROM ST_Read('{SHP}') WHERE ST_Area(geom) > 1e7
""").fetchone()
print(f"  loaded {con.execute('SELECT COUNT(*) FROM lc2008').fetchone()[0]:,} features   (excluded {excluded[0]} outlier polygons, {excluded[1]/1e6:.1f} km²)")

# Neighbourhoods in WGS84
con.execute(f"""
    CREATE TABLE nbhd AS
    SELECT AREA_SHORT_CODE AS code, AREA_NAME AS name, geom
    FROM ST_Read('{NBHD}')
""")
print(f"  neighbourhoods: {con.execute('SELECT COUNT(*) FROM nbhd').fetchone()[0]}")

print("Spatial join (point-in-polygon)...")
con.execute("""
    CREATE TABLE joined AS
    SELECT lc.gridcode, lc.area_m2, n.name AS nbhd_name
    FROM lc2008 lc
    LEFT JOIN nbhd n ON ST_Within(lc.centroid_wgs84, n.geom)
""")
total = con.execute("SELECT COUNT(*) FROM joined").fetchone()[0]
unmatched = con.execute("SELECT COUNT(*) FROM joined WHERE nbhd_name IS NULL").fetchone()[0]
print(f"  {total:,} joined   {unmatched:,} outside nbhd polygons")

# gridcode mapping in 2008: based on notes field in CKAN page
# 1=tree canopy, 2=grass/shrub, 3=bare earth, 4=water, 5=buildings, 6=roads, 7=other paved, 8=agriculture
CLASS_MAP = {
    1: "tree", 2: "grass_shrub", 3: "bare", 4: "water",
    5: "building", 6: "road", 7: "other_paved", 8: "agriculture",
}
case = "CASE " + " ".join(f"WHEN gridcode={k} THEN '{v}'" for k, v in CLASS_MAP.items()) + " ELSE 'unknown' END"

print("Aggregating by neighbourhood × class...")
con.execute(f"""
    CREATE TABLE agg2008 AS
    SELECT nbhd_name, {case} AS cls, SUM(area_m2) AS area_m2
    FROM joined WHERE nbhd_name IS NOT NULL
    GROUP BY 1, 2
""")
con.execute(f"COPY agg2008 TO '{OUT}' (FORMAT PARQUET)")
print(f"wrote {OUT}")

# Peek
print("\ncitywide class totals (2008):")
print(con.execute("""
    SELECT cls, ROUND(SUM(area_m2)/1e6, 2) AS km2,
           ROUND(100.0*SUM(area_m2)/SUM(SUM(area_m2)) OVER (), 2) AS pct
    FROM agg2008 GROUP BY 1 ORDER BY SUM(area_m2) DESC
""").fetchdf().to_string(index=False))
