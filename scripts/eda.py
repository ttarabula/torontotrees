"""First-look EDA on Street Tree Data (CSV, WGS84). Prints to stdout."""
from pathlib import Path
import duckdb

CSV = Path(__file__).resolve().parent.parent / "data" / "raw" / "street-tree-data-4326.csv"
con = duckdb.connect()
con.execute(f"CREATE VIEW t AS SELECT * FROM read_csv_auto('{CSV}', SAMPLE_SIZE=200000)")


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def run(sql: str, limit: int | None = None) -> None:
    df = con.execute(sql).fetchdf()
    if limit is not None:
        df = df.head(limit)
    print(df.to_string(index=False))


section("shape + nulls")
run("""
    SELECT 'rows' AS metric, COUNT(*)::VARCHAR AS value FROM t
    UNION ALL SELECT 'null botanical', SUM(BOTANICAL_NAME IS NULL)::VARCHAR FROM t
    UNION ALL SELECT 'null common',    SUM(COMMON_NAME    IS NULL)::VARCHAR FROM t
    UNION ALL SELECT 'null ward',      SUM(WARD           IS NULL)::VARCHAR FROM t
    UNION ALL SELECT 'null dbh',       SUM(DBH_TRUNK      IS NULL)::VARCHAR FROM t
    UNION ALL SELECT 'null geometry',  SUM(geometry       IS NULL)::VARCHAR FROM t
""")

section("top 25 botanical names")
run("""
    SELECT BOTANICAL_NAME, COUNT(*) AS n
    FROM t GROUP BY 1 ORDER BY n DESC LIMIT 25
""")

section("top 25 common names")
run("""
    SELECT COMMON_NAME, COUNT(*) AS n
    FROM t GROUP BY 1 ORDER BY n DESC LIMIT 25
""")

section("distinct species counts")
run("""
    SELECT
        COUNT(DISTINCT BOTANICAL_NAME) AS distinct_botanical,
        COUNT(DISTINCT COMMON_NAME)    AS distinct_common,
        COUNT(DISTINCT LOWER(TRIM(BOTANICAL_NAME))) AS distinct_botanical_norm,
        COUNT(DISTINCT LOWER(TRIM(COMMON_NAME)))    AS distinct_common_norm
    FROM t
""")

section("common-name spelling collisions (same lower/trim, >1 raw variant)")
run("""
    WITH norm AS (
        SELECT LOWER(TRIM(COMMON_NAME)) AS k, COMMON_NAME AS raw, COUNT(*) AS n
        FROM t WHERE COMMON_NAME IS NOT NULL
        GROUP BY 1,2
    )
    SELECT k, COUNT(*) AS variants, STRING_AGG(raw || ' (' || n || ')', ' | ') AS examples
    FROM norm GROUP BY k HAVING COUNT(*) > 1
    ORDER BY variants DESC LIMIT 15
""")

section("DBH_TRUNK distribution (cm assumed)")
run("""
    SELECT
        COUNT(*)                              AS n,
        MIN(DBH_TRUNK)                        AS min,
        approx_quantile(DBH_TRUNK, 0.25)      AS p25,
        approx_quantile(DBH_TRUNK, 0.50)      AS p50,
        approx_quantile(DBH_TRUNK, 0.75)      AS p75,
        approx_quantile(DBH_TRUNK, 0.95)      AS p95,
        approx_quantile(DBH_TRUNK, 0.99)      AS p99,
        MAX(DBH_TRUNK)                        AS max,
        SUM(DBH_TRUNK = 0)                    AS zero_count,
        SUM(DBH_TRUNK > 200)                  AS over_200_count
    FROM t WHERE DBH_TRUNK IS NOT NULL
""")

section("ward coverage")
run("""
    SELECT WARD, COUNT(*) AS n
    FROM t GROUP BY 1 ORDER BY n DESC
""")

section("bbox (lat/lon parsed from GeoJSON MultiPoint)")
run("""
    WITH p AS (
        SELECT
            json_extract(geometry, '$.coordinates[0][0]')::DOUBLE AS lon,
            json_extract(geometry, '$.coordinates[0][1]')::DOUBLE AS lat
        FROM t WHERE geometry IS NOT NULL
    )
    SELECT MIN(lat) AS min_lat, MAX(lat) AS max_lat,
           MIN(lon) AS min_lon, MAX(lon) AS max_lon,
           SUM(lat IS NULL) AS unparsed
    FROM p
""")

section("rare species (1 occurrence)")
run("""
    WITH c AS (SELECT BOTANICAL_NAME, COUNT(*) AS n FROM t GROUP BY 1)
    SELECT
        SUM(n = 1)   AS singleton_species,
        SUM(n <= 5)  AS leq_5_species,
        SUM(n <= 10) AS leq_10_species,
        COUNT(*)     AS total_species
    FROM c
""")

section("top 10 streets by tree count")
run("""
    SELECT STREETNAME, COUNT(*) AS n
    FROM t WHERE STREETNAME IS NOT NULL
    GROUP BY 1 ORDER BY n DESC LIMIT 10
""")
