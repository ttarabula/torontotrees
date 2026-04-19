"""Heat-risk proxy per neighbourhood from 2018 land-cover + canopy.

Research on urban heat islands consistently finds that summer land-surface
temperature is well-predicted by the share of a neighbourhood covered by
impervious hard surfaces (buildings, roads, parking) minus the share
covered by tree canopy. Without direct thermal imagery, we can compute a
heat-risk proxy from the land-cover components we already have.

Formula (simple, transparent):
    heat_proxy = impervious_pct - canopy_pct
where
    impervious_pct = building + road + other paved + bare

This won't give absolute temperatures. It will give a defensible
ordinal ranking — "which neighbourhoods are hottest" — with
correlation to actual LST typically r > 0.85 in published studies.

Writes data/processed/nbhd_heat.parquet and prints findings.
"""
from pathlib import Path
import re
import duckdb
import pyogrio
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
GDB = ROOT / "data" / "raw" / "landcover" / "LandCover2018.gdb"
CANOPY = ROOT / "data" / "processed" / "nbhd_canopy.parquet"
OUT = ROOT / "data" / "processed" / "nbhd_heat.parquet"


def slug(s): return re.sub(r"[^a-z0-9]", "", str(s).lower())


print("loading 2018 land cover…")
lc = pyogrio.read_dataframe(str(GDB), layer="LandCover2018", read_geometry=False)
lc = lc[lc.HoodName != ""]
agg = lc.groupby(["HoodName", "Desc"])["Shape_Area"].sum().unstack(fill_value=0)
agg["total_m2"] = agg.sum(axis=1)

# Impervious: buildings + roads + other paved + bare (bare is often parking lots / gravel)
agg["impervious_m2"] = (
    agg.get("building", 0) + agg.get("road", 0) +
    agg.get("other", 0) + agg.get("bare", 0)
)
agg["impervious_pct"] = 100 * agg["impervious_m2"] / agg["total_m2"]
agg["canopy_pct"] = 100 * agg.get("tree", 0) / agg["total_m2"]
agg["green_pct"] = 100 * (agg.get("tree", 0) + agg.get("grass", 0) + agg.get("shrub", 0)) / agg["total_m2"]
agg["heat_proxy"] = agg["impervious_pct"] - agg["canopy_pct"]
heat = agg[["impervious_pct", "canopy_pct", "green_pct", "heat_proxy", "total_m2"]].reset_index().rename(columns={"HoodName": "nbhd_name"})

# Merge with canopy summary (income, population, classification, trees_per_capita)
con = duckdb.connect()
meta = con.execute(f"SELECT * FROM read_parquet('{CANOPY}')").fetchdf()
meta["_k"] = meta.nbhd_name.map(slug)
heat["_k"] = heat.nbhd_name.map(slug)
merged = heat.merge(
    meta.drop(columns=["canopy_pct","tree_shrub_pct","total_m2"], errors="ignore"),
    on="_k", how="inner", suffixes=("", "_meta")
)
merged = merged.drop(columns=[c for c in merged.columns if c.endswith("_meta")])
merged.to_parquet(OUT, index=False)
print(f"wrote {OUT} — {len(merged)} nbhds")

print("\n--- HOTTEST neighbourhoods (by heat proxy) ---")
print(merged.nlargest(12, "heat_proxy")[
    ["nbhd_name","impervious_pct","canopy_pct","heat_proxy","median_hh_income","classification"]
].round(1).to_string(index=False))

print("\n--- COOLEST neighbourhoods (lowest heat proxy) ---")
print(merged.nsmallest(12, "heat_proxy")[
    ["nbhd_name","impervious_pct","canopy_pct","heat_proxy","median_hh_income","classification"]
].round(1).to_string(index=False))

# Correlations with income
m = merged.dropna(subset=["median_hh_income"])
print(f"\nheat_proxy vs income:          r = {m['heat_proxy'].corr(m['median_hh_income']):.3f}")
print(f"impervious_pct vs income:      r = {m['impervious_pct'].corr(m['median_hh_income']):.3f}")
print(f"canopy_pct vs income:          r = {m['canopy_pct'].corr(m['median_hh_income']):.3f}")

# NIA bucket
def bucket(c):
    c = str(c)
    if "Improvement" in c: return "NIA"
    if c.strip() == "Emerging Neighbourhood": return "Emerging"
    return "Not NIA/Emerging"
merged["bucket"] = merged["classification"].map(bucket)
print("\n--- average heat proxy by NIA bucket ---")
print(merged.groupby("bucket").agg(
    n=("nbhd_name","count"),
    avg_heat_proxy=("heat_proxy","mean"),
    avg_impervious=("impervious_pct","mean"),
    avg_canopy=("canopy_pct","mean"),
    avg_hh_income=("median_hh_income","mean"),
).round(2).to_string())
