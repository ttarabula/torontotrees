"""Analytics over the neighbourhood-level table.

Prints:
  - income vs tree density correlation
  - top/bottom neighbourhoods by trees/km², trees/capita, Shannon
  - equity summary by TSNS classification (NIA vs. not)
  - monoculture check: top-species share per neighbourhood
"""
from pathlib import Path
import duckdb

OUT = Path(__file__).resolve().parent.parent / "data" / "processed"
con = duckdb.connect()
con.execute(f"CREATE VIEW s AS SELECT * FROM read_parquet('{OUT/'nbhd_summary.parquet'}')")
con.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{OUT/'trees.parquet'}')")


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def show(sql: str) -> None:
    print(con.execute(sql).fetchdf().to_string(index=False))


section("income vs tree density — correlation")
show("""
    SELECT
        corr(median_hh_income, trees_per_km2)     AS r_income_density,
        corr(median_hh_income, trees_per_capita)  AS r_income_percap,
        corr(median_hh_income, shannon_h)         AS r_income_shannon,
        corr(ln(median_hh_income), ln(trees_per_km2)) AS r_loglog_income_density
    FROM s
    WHERE median_hh_income IS NOT NULL AND trees_per_km2 > 0
""")

section("top 10 neighbourhoods by trees/km²")
show("""
    SELECT nbhd_name, ROUND(trees_per_km2)::INT AS per_km2,
           tree_count, ROUND(area_km2,2) AS km2,
           median_hh_income AS hh_inc,
           classification
    FROM s ORDER BY trees_per_km2 DESC LIMIT 10
""")

section("bottom 10 neighbourhoods by trees/km²")
show("""
    SELECT nbhd_name, ROUND(trees_per_km2)::INT AS per_km2,
           tree_count, ROUND(area_km2,2) AS km2,
           median_hh_income AS hh_inc,
           classification
    FROM s ORDER BY trees_per_km2 ASC LIMIT 10
""")

section("top 10 by trees per capita")
show("""
    SELECT nbhd_name, ROUND(trees_per_capita,3) AS per_capita,
           tree_count, population_2021, median_hh_income AS hh_inc
    FROM s WHERE population_2021 > 0
    ORDER BY trees_per_capita DESC LIMIT 10
""")

section("bottom 10 by trees per capita")
show("""
    SELECT nbhd_name, ROUND(trees_per_capita,3) AS per_capita,
           tree_count, population_2021, median_hh_income AS hh_inc
    FROM s WHERE population_2021 > 0
    ORDER BY trees_per_capita ASC LIMIT 10
""")

section("equity by TSNS classification (NIA = Neighbourhood Improvement Area)")
show("""
    SELECT
        CASE
            WHEN classification ILIKE '%Neighbourhood Improvement%' THEN 'NIA'
            WHEN classification ILIKE '%Emerging%' THEN 'Emerging'
            ELSE 'Not NIA/Emerging'
        END AS bucket,
        COUNT(*) AS n,
        ROUND(AVG(trees_per_km2))::INT AS avg_trees_per_km2,
        ROUND(AVG(trees_per_capita),3) AS avg_trees_per_capita,
        ROUND(AVG(median_hh_income))::INT AS avg_hh_income,
        ROUND(AVG(shannon_h),2) AS avg_shannon
    FROM s GROUP BY 1 ORDER BY 1
""")

section("top 10 most diverse (Shannon H)")
show("""
    SELECT nbhd_name, ROUND(shannon_h,2) AS H,
           species_count, tree_count, top_species,
           ROUND(top_species_share*100,1) AS pct_top_species
    FROM s ORDER BY shannon_h DESC LIMIT 10
""")

section("top 10 most monocultural (lowest Shannon H among non-tiny)")
show("""
    SELECT nbhd_name, ROUND(shannon_h,2) AS H,
           species_count, tree_count, top_species,
           ROUND(top_species_share*100,1) AS pct_top_species
    FROM s WHERE tree_count >= 500
    ORDER BY shannon_h ASC LIMIT 10
""")

section("city-wide top species concentration")
show("""
    WITH sp AS (
        SELECT botanical_key, COUNT(*) AS n FROM t GROUP BY 1
    ),
    total AS (SELECT SUM(n) AS total FROM sp)
    SELECT botanical_key, n, ROUND(100.0 * n / total, 2) AS pct
    FROM sp, total ORDER BY n DESC LIMIT 10
""")

section("spearman rank of income vs density (via rank())")
# DuckDB doesn't have corr_spearman, so compute via ranks
show("""
    WITH r AS (
        SELECT
            rank() OVER (ORDER BY median_hh_income)   AS ri,
            rank() OVER (ORDER BY trees_per_km2)      AS rd
        FROM s WHERE median_hh_income IS NOT NULL
    )
    SELECT ROUND(corr(ri, rd), 3) AS spearman_income_density FROM r
""")
