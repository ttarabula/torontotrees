"""Merge 2018 canopy data with our neighbourhood summary (street trees + income).

Outputs:
  data/processed/nbhd_canopy.parquet  — one row per neighbourhood, with canopy%,
    trees_per_capita, median_hh_income, classification.
  Also prints: top/bottom canopy, equity correlations, NIA comparison,
  and the street-tree-share-of-canopy ratio.
"""
from pathlib import Path
import re
import duckdb
import pyogrio
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
GDB = ROOT / "data" / "raw" / "landcover" / "LandCover2018.gdb"
OUT = ROOT / "data" / "processed" / "nbhd_canopy.parquet"

print("loading 2018 land cover polygons...")
lc = pyogrio.read_dataframe(str(GDB), layer="LandCover2018", read_geometry=False)
lc = lc[lc.HoodName != ""]
agg = lc.groupby(["HoodName", "Desc"])["Shape_Area"].sum().unstack(fill_value=0)
agg["total_m2"] = agg.sum(axis=1)
agg["canopy_pct"] = 100 * agg.get("tree", 0) / agg["total_m2"]
agg["tree_shrub_pct"] = 100 * (agg.get("tree", 0) + agg.get("shrub", 0)) / agg["total_m2"]
canopy = agg[["canopy_pct", "tree_shrub_pct", "total_m2"]].reset_index().rename(columns={"HoodName": "nbhd_name"})
print(f"canopy rows: {len(canopy)}  citywide canopy%: {100*agg.get('tree', 0).sum()/agg['total_m2'].sum():.2f}")

print("loading street-tree nbhd summary...")
con = duckdb.connect()
summary = con.execute(f"SELECT * FROM read_parquet('{ROOT/'data/processed/nbhd_summary.parquet'}')").fetchdf()


def slug(s):
    return re.sub(r"[^a-z0-9]", "", s.lower())


summary["_k"] = summary["nbhd_name"].map(slug)
canopy["_k"] = canopy["nbhd_name"].map(slug)
merged = canopy.merge(summary, on="_k", how="inner", suffixes=("_c", "_s"))
print(f"merged rows: {len(merged)}  unmatched canopy: {len(canopy) - len(merged)}  unmatched summary: {len(summary) - len(merged)}")
if len(canopy) - len(merged):
    print("  canopy nbhds not in summary:")
    cns = set(canopy._k); sns = set(summary._k)
    for k in cns - sns:
        nm = canopy.loc[canopy._k == k, 'nbhd_name'].iloc[0]
        print(f"    {nm}")

# Keep a tidy frame
df = merged[[
    "nbhd_name_c", "classification", "canopy_pct", "tree_shrub_pct", "total_m2",
    "tree_count", "trees_per_km2", "trees_per_capita", "species_count", "shannon_h",
    "top_species", "top_species_share", "median_hh_income", "population_2021",
]].rename(columns={"nbhd_name_c": "nbhd_name"})
df.to_parquet(OUT, index=False)
print(f"wrote {OUT}")

# === analysis ===
def show(title, d):
    print(f"\n=== {title} ===")
    print(d.to_string(index=False))


print("\n"+"="*60+"\nCANOPY FINDINGS\n"+"="*60)
print(f"citywide canopy cover (tree class only): {df.canopy_pct.mean():.1f}% (neighbourhood mean)")
print(f"citywide canopy (area-weighted): {100 * (df.canopy_pct/100 * df.total_m2).sum() / df.total_m2.sum():.2f}%")

show("top 10 by canopy %", df.nlargest(10, "canopy_pct")[["nbhd_name","canopy_pct","median_hh_income","classification"]].round({"canopy_pct":1}))
show("bottom 10 by canopy %", df.nsmallest(10, "canopy_pct")[["nbhd_name","canopy_pct","median_hh_income","classification"]].round({"canopy_pct":1}))

print("\n=== equity — canopy vs street-tree metrics vs income ===")
print(f"canopy %   vs income:         r = {df['canopy_pct'].corr(df['median_hh_income']):.3f}")
print(f"trees/km²  vs income:         r = {df['trees_per_km2'].corr(df['median_hh_income']):.3f}")
print(f"trees/capita vs income:       r = {df['trees_per_capita'].corr(df['median_hh_income']):.3f}")
print(f"canopy %   vs trees/capita:   r = {df['canopy_pct'].corr(df['trees_per_capita']):.3f}")
print(f"canopy %   vs trees/km²:      r = {df['canopy_pct'].corr(df['trees_per_km2']):.3f}")

def bucket(c):
    c = str(c)
    if "Improvement" in c:
        return "NIA"
    if c.strip() == "Emerging Neighbourhood":
        return "Emerging"
    return "Not NIA/Emerging"


df["bucket"] = df["classification"].map(bucket)
print("\n=== by NIA bucket ===")
g = df.groupby("bucket").agg(
    n=("nbhd_name","count"),
    avg_canopy_pct=("canopy_pct","mean"),
    avg_trees_per_capita=("trees_per_capita","mean"),
    avg_hh_income=("median_hh_income","mean"),
).round(2)
print(g.to_string())

# Street-tree share of overall canopy (rough): street tree count / total trees (from topographic layer)
# We only know the street-tree count directly; to estimate "share of canopy", compute:
# average crown area per street tree ~ 50 m² (varies) — rough estimate.
# Instead, compare: for a given canopy%, how many street trees per m² of canopy?
print("\n=== street trees relative to canopy ===")
print("trees per 1000 m² of canopy, by NIA bucket:")
df["canopy_m2"] = df["canopy_pct"] / 100 * df["total_m2"]
g2 = df.groupby("bucket").apply(lambda x: 1000 * x.tree_count.sum() / x.canopy_m2.sum()).round(2)
print(g2.to_string())
