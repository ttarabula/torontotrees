"""Estimate annual ecosystem-service value of Toronto's street trees.

Approach:
- Assign each tree a species group (large/medium/small broadleaf, conifer, other)
- Per-tree annual ecosystem-service value = K * DBH^1.5  (USD/year)
  where K varies by species group. Calibrated so a 50-cm medium broadleaf
  produces ~$60/yr, consistent with i-Tree Streets figures from US
  Midwest/Northeast community-tree guides (McPherson et al.).
- This is a "utility value" estimate: CO2 + stormwater + air-pollutant
  removal + energy savings. It excludes property-value effects and is
  NOT a substitute for the city's own i-Tree Eco study.

Writes:
  data/processed/tree_value.parquet  — per-tree dollar value
  data/processed/nbhd_value.parquet  — per-neighbourhood aggregates
Also prints a summary.
"""
from pathlib import Path
import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
TREES = ROOT / "data" / "processed" / "trees.parquet"
SUMMARY = ROOT / "data" / "processed" / "nbhd_canopy.parquet"
OUT_TREE = ROOT / "data" / "processed" / "tree_value.parquet"
OUT_NBHD = ROOT / "data" / "processed" / "nbhd_value.parquet"

K = {
    "large_broadleaf":  0.28,
    "medium_broadleaf": 0.17,
    "small_broadleaf":  0.08,
    "conifer":          0.12,
    "other":            0.14,
}

# Map common-name stems to species groups. The common_raw field typically
# reads "Maple, Norway" / "Oak, red" / "Pine, Austrian" etc.
SPECIES_GROUPS = {
    "large_broadleaf": [
        "oak", "linden", "basswood", "elm", "ash", "hackberry",
        "catalpa", "london plane", "sycamore", "tulip", "beech",
        "coffeetree", "walnut", "butternut", "pecan",
    ],
    "medium_broadleaf": [
        "maple", "honey locust", "black locust", "birch", "horsechestnut",
        "buckeye", "mulberry", "poplar", "willow", "hickory",
        "zelkova", "yellowwood", "ginkgo", "katsura", "sassafras",
        "ironwood", "hop tree", "hophornbeam", "hornbeam",
    ],
    "small_broadleaf": [
        "crabapple", "apple", "cherry", "plum", "peach", "apricot",
        "serviceberry", "dogwood", "redbud", "lilac", "hawthorn",
        "mountain ash", "pear", "almond", "elder", "magnolia",
        "hazel", "buckthorn", "sumac", "burning bush", "spiraea",
    ],
    "conifer": [
        "spruce", "pine", "fir", "cedar", "thuja", "hemlock",
        "yew", "juniper", "larch", "tamarack", "cypress",
    ],
}

def species_group(common: str | None) -> str:
    if not common:
        return "other"
    c = common.lower()
    for group, kws in SPECIES_GROUPS.items():
        if any(kw in c for kw in kws):
            return group
    return "other"


print("loading trees…")
con = duckdb.connect()
con.execute(f"CREATE VIEW t AS SELECT * FROM read_parquet('{TREES}')")
df = con.execute("""
    SELECT _id, common_raw, botanical_raw, dbh_cm, nbhd_name, WARD
    FROM t
    WHERE dbh_cm IS NOT NULL AND dbh_cm > 0
""").fetchdf()
print(f"  {len(df):,} trees with DBH data")

df["group"] = df["common_raw"].map(species_group)
df["k"] = df["group"].map(K)
df["annual_usd"] = df["k"] * (df["dbh_cm"] ** 1.5)
total = df["annual_usd"].sum()
print(f"\ngroup totals (USD/year):")
print(df.groupby("group")["annual_usd"].agg(["count","sum","mean"]).round(2).to_string())
print(f"\nTOTAL annual value: ${total/1e6:.2f}M")
print(f"Mean per tree: ${df['annual_usd'].mean():.2f}")
print(f"Median per tree: ${df['annual_usd'].median():.2f}")

# Top species by total value
print("\ntop 10 species by total annual value:")
print(df.groupby("common_raw")["annual_usd"].agg(["count","sum","mean"])
        .sort_values("sum", ascending=False).head(10).round(2).to_string())

# Per-neighbourhood aggregate
print("\nbuilding per-neighbourhood aggregates…")
nbhd = df.groupby("nbhd_name").agg(
    trees_valued=("_id","count"),
    total_usd=("annual_usd","sum"),
    avg_per_tree_usd=("annual_usd","mean"),
).reset_index()

# Join income + population from nbhd_canopy summary
meta = con.execute(f"""
    SELECT nbhd_name, canopy_pct, median_hh_income, population_2021,
           classification, tree_count, trees_per_capita
    FROM read_parquet('{SUMMARY}')
""").fetchdf()

merged = nbhd.merge(meta, on="nbhd_name", how="left")
merged["usd_per_capita"] = merged["total_usd"] / merged["population_2021"]
merged.to_parquet(OUT_NBHD, index=False)
df[["_id","common_raw","dbh_cm","group","annual_usd","nbhd_name"]].to_parquet(OUT_TREE, index=False)
print(f"wrote {OUT_NBHD} and {OUT_TREE}")

# Key findings
print("\n--- top 10 neighbourhoods by TOTAL annual value ---")
print(merged.nlargest(10, "total_usd")[["nbhd_name","trees_valued","total_usd","median_hh_income","classification"]]
        .round(0).to_string(index=False))
print("\n--- top 10 by PER-CAPITA annual value ---")
print(merged.dropna(subset=["population_2021"])
        .sort_values("usd_per_capita", ascending=False).head(10)
        [["nbhd_name","trees_valued","population_2021","usd_per_capita","median_hh_income","classification"]]
        .round(1).to_string(index=False))
print("\n--- bottom 10 by PER-CAPITA annual value ---")
print(merged.dropna(subset=["population_2021"])
        .sort_values("usd_per_capita").head(10)
        [["nbhd_name","trees_valued","population_2021","usd_per_capita","median_hh_income","classification"]]
        .round(1).to_string(index=False))

# Equity: correlation with income
m = merged.dropna(subset=["median_hh_income","usd_per_capita"])
r = m["median_hh_income"].corr(m["usd_per_capita"])
print(f"\ncorrelation: per-capita value vs. income = {r:.3f}")

# NIA comparison
def bucket(c):
    c = str(c)
    if "Improvement" in c: return "NIA"
    if c.strip() == "Emerging Neighbourhood": return "Emerging"
    return "Not NIA/Emerging"

merged["bucket"] = merged["classification"].map(bucket)
print("\nper-capita ecosystem-service value by NIA bucket:")
print(merged.dropna(subset=["population_2021"])
        .groupby("bucket").agg(
            n=("nbhd_name","count"),
            avg_per_capita_usd=("usd_per_capita","mean"),
            avg_hh_income=("median_hh_income","mean"),
        ).round(2).to_string())
