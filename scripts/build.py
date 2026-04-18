"""Build the enriched trees parquet + a neighbourhood-level table.

Steps:
  1. Parse lat/lon from GeoJSON geometry, normalize species, cap DBH, spatial-join to neighbourhood.
  2. Extract 2021 median household income per neighbourhood from the census XLSX.
  3. Compute per-neighbourhood aggregates (tree count, area km², Shannon index, top species).

Outputs to data/processed/.
"""
from pathlib import Path
import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
OUT = ROOT / "data" / "processed"
OUT.mkdir(parents=True, exist_ok=True)

TREES_CSV = RAW / "street-tree-data-4326.csv"
NBHD_GEOJSON = RAW / "neighbourhoods-4326.geojson"
CENSUS_XLSX = RAW / "nbhd_2021_census_profile_full_158model.xlsx"

DBH_CAP_CM = 250  # anything larger is almost certainly bad data

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")


def step(msg: str) -> None:
    print(f"\n>> {msg}")


step("load trees, parse lat/lon, normalize species, cap DBH")
con.execute(f"""
    CREATE TABLE trees_raw AS
    SELECT
        _id, OBJECTID, STRUCTID,
        ADDRESS, STREETNAME, CROSSSTREET1, CROSSSTREET2, SUFFIX,
        UNIT_NUMBER, TREE_POSITION_NUMBER, SITE, WARD,
        BOTANICAL_NAME AS botanical_raw,
        COMMON_NAME    AS common_raw,
        DBH_TRUNK      AS dbh_raw,
        json_extract(geometry, '$.coordinates[0][0]')::DOUBLE AS lon,
        json_extract(geometry, '$.coordinates[0][1]')::DOUBLE AS lat
    FROM read_csv_auto('{TREES_CSV}', SAMPLE_SIZE=200000)
""")

con.execute(f"""
    CREATE TABLE trees_clean AS
    SELECT
        _id, OBJECTID,
        ADDRESS, STREETNAME, SUFFIX, WARD,
        -- species normalization: trim, lower, then title-case for display
        trim(lower(botanical_raw))         AS botanical_key,
        trim(lower(common_raw))            AS common_key,
        botanical_raw, common_raw,
        CASE WHEN dbh_raw BETWEEN 1 AND {DBH_CAP_CM} THEN dbh_raw END AS dbh_cm,
        lon, lat,
        ST_Point(lon, lat) AS geom
    FROM trees_raw
""")
n = con.execute("SELECT COUNT(*) FROM trees_clean").fetchone()[0]
print(f"  trees_clean: {n:,}")

step("load neighbourhoods (158 model)")
con.execute(f"""
    CREATE TABLE nbhd AS
    SELECT
        AREA_SHORT_CODE AS nbhd_code,
        AREA_NAME       AS nbhd_name,
        CLASSIFICATION  AS classification,
        geom
    FROM ST_Read('{NBHD_GEOJSON}')
""")
nb = con.execute("SELECT COUNT(*) FROM nbhd").fetchone()[0]
print(f"  nbhd polygons: {nb}")

step("spatial join: assign each tree to a neighbourhood")
con.execute("""
    CREATE TABLE trees AS
    SELECT t.* EXCLUDE (geom),
           n.nbhd_code, n.nbhd_name
    FROM trees_clean t
    LEFT JOIN nbhd n
      ON ST_Within(t.geom, n.geom)
""")
joined = con.execute("""
    SELECT SUM(nbhd_code IS NOT NULL) AS assigned,
           SUM(nbhd_code IS NULL)     AS unassigned,
           COUNT(*)                   AS total
    FROM trees
""").fetchdf()
print(joined.to_string(index=False))

step("write trees parquet")
trees_pq = OUT / "trees.parquet"
con.execute(f"COPY trees TO '{trees_pq}' (FORMAT PARQUET)")
print(f"  -> {trees_pq} ({trees_pq.stat().st_size/1e6:.1f} MB)")

step("compute neighbourhood area (km²)")
con.execute("""
    CREATE TABLE nbhd_area AS
    SELECT
        nbhd_code, nbhd_name, classification,
        ST_Area_Spheroid(geom) / 1e6 AS area_km2
    FROM nbhd
""")

step("extract 2021 median household income per neighbourhood from XLSX")
# The sheet has neighbourhoods as COLUMNS. Row 243 (0-indexed) in col 0 = "Median total income of household in 2020 ($)".
census = pd.read_excel(CENSUS_XLSX, sheet_name="hd2021_census_profile")
# Identify the header row (first data row has neighbourhood names in columns 1+, first col label "Neighbourhood Name")
assert census.columns[0] == "Neighbourhood Name"
# row index of median household income
label_col = census["Neighbourhood Name"].astype(str).str.strip()
idx_income = label_col.str.contains("Median total income of household in 2020", na=False)
assert idx_income.sum() == 1, f"expected 1 income row, got {idx_income.sum()}"
idx_pop = label_col.str.fullmatch(
    r"Total - Age groups of the population - 25% sample data", na=False
)
assert idx_pop.sum() == 1, f"expected 1 pop row, got {idx_pop.sum()}"
income_row = census.loc[idx_income].iloc[0]
pop_row = census.loc[idx_pop].iloc[0]

records = []
for col in census.columns[1:]:
    records.append({
        "nbhd_name": col,
        "median_hh_income": pd.to_numeric(income_row[col], errors="coerce"),
        "population_2021": pd.to_numeric(pop_row[col], errors="coerce"),
    })
demog = pd.DataFrame(records)
print(f"  {len(demog)} neighbourhoods; income non-null: {demog.median_hh_income.notna().sum()}")

con.register("demog_df", demog)
con.execute("CREATE TABLE demog AS SELECT * FROM demog_df")

step("build neighbourhood summary (count, area, density, shannon, top species)")
con.execute("""
    CREATE TABLE nbhd_summary AS
    WITH counts AS (
        SELECT nbhd_code, nbhd_name, COUNT(*) AS tree_count
        FROM trees WHERE nbhd_code IS NOT NULL
        GROUP BY 1,2
    ),
    species_counts AS (
        SELECT nbhd_code, botanical_key, COUNT(*) AS n
        FROM trees WHERE nbhd_code IS NOT NULL
        GROUP BY 1,2
    ),
    shannon AS (
        SELECT sc.nbhd_code,
               -SUM((sc.n::DOUBLE / c.tree_count) * ln(sc.n::DOUBLE / c.tree_count)) AS shannon_h,
               COUNT(*) AS species_count
        FROM species_counts sc
        JOIN counts c USING (nbhd_code)
        GROUP BY 1
    ),
    top_species AS (
        SELECT nbhd_code,
               arg_max(botanical_key, n) AS top_species,
               MAX(n)::DOUBLE / ANY_VALUE(tree_count) AS top_species_share
        FROM species_counts
        JOIN counts USING (nbhd_code)
        GROUP BY 1
    )
    SELECT
        c.nbhd_code, c.nbhd_name,
        a.classification,
        a.area_km2,
        c.tree_count,
        c.tree_count / a.area_km2 AS trees_per_km2,
        d.median_hh_income,
        d.population_2021,
        c.tree_count::DOUBLE / NULLIF(d.population_2021, 0) AS trees_per_capita,
        s.species_count,
        s.shannon_h,
        ts.top_species,
        ts.top_species_share
    FROM counts c
    LEFT JOIN nbhd_area a USING (nbhd_code)
    LEFT JOIN demog d ON regexp_replace(lower(d.nbhd_name), '[^a-z0-9]', '', 'g')
                       = regexp_replace(lower(c.nbhd_name), '[^a-z0-9]', '', 'g')
    LEFT JOIN shannon s USING (nbhd_code)
    LEFT JOIN top_species ts USING (nbhd_code)
    ORDER BY trees_per_km2 DESC
""")

# Report join success
match = con.execute("""
    SELECT
        SUM(median_hh_income IS NOT NULL) AS income_matched,
        SUM(median_hh_income IS NULL)     AS income_missing,
        COUNT(*)                          AS total_nbhd
    FROM nbhd_summary
""").fetchdf()
print(match.to_string(index=False))

if match.iloc[0]["income_missing"] > 0:
    print("\n  UNMATCHED nbhds:")
    u = con.execute("""
        SELECT c.nbhd_name FROM nbhd_summary c
        WHERE c.median_hh_income IS NULL
        ORDER BY 1
    """).fetchdf()
    print(u.to_string(index=False))

step("write neighbourhood parquet")
nbhd_pq = OUT / "nbhd_summary.parquet"
con.execute(f"COPY nbhd_summary TO '{nbhd_pq}' (FORMAT PARQUET)")
print(f"  -> {nbhd_pq} ({nbhd_pq.stat().st_size/1e3:.1f} KB)")

# Also export neighbourhood polygons for later use
nbhd_geo_pq = OUT / "nbhd_polygons.parquet"
con.execute(f"""
    COPY (SELECT nbhd_code, nbhd_name, classification, ST_AsWKB(geom) AS geom_wkb
          FROM nbhd) TO '{nbhd_geo_pq}' (FORMAT PARQUET)
""")
print(f"  -> {nbhd_geo_pq} ({nbhd_geo_pq.stat().st_size/1e3:.1f} KB)")
