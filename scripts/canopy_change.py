"""Compare 2008 vs 2018 canopy per neighbourhood. Produce change tables + charts."""
from pathlib import Path
import re
import duckdb
import pyogrio
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon as MplPoly
from shapely import wkb

ROOT = Path(__file__).resolve().parent.parent
CHARTS = ROOT / "site" / "charts"
CHARTS.mkdir(parents=True, exist_ok=True)


def slug(s):
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


# 2018: wide-format canopy summary already exists
con = duckdb.connect()
y2018 = con.execute(f"""
    SELECT nbhd_name, canopy_pct AS canopy_2018, total_m2, median_hh_income,
           population_2021, trees_per_capita, classification, tree_count
    FROM read_parquet('{ROOT/'data/processed/nbhd_canopy.parquet'}')
""").fetchdf()

# 2008 long-format → canopy % per neighbourhood
lc2008 = con.execute(f"""
    SELECT nbhd_name, cls, area_m2
    FROM read_parquet('{ROOT/'data/processed/nbhd_canopy_2008.parquet'}')
""").fetchdf()
y2008 = lc2008.pivot_table(index="nbhd_name", columns="cls", values="area_m2", fill_value=0).reset_index()

# Use 2018 total_m2 as the consistent denominator for both years —
# avoids data-encoding artifacts in the 2008 road polygons.
y2018["_k"] = y2018.nbhd_name.map(slug)
y2008["_k"] = y2008.nbhd_name.map(slug)
merged = y2018.merge(y2008[["_k", "tree", "grass_shrub", "bare"]],
                      on="_k", how="inner", suffixes=("", "_08"))
merged["canopy_2008"] = 100 * merged["tree"] / merged["total_m2"]
merged["change"] = merged["canopy_2018"] - merged["canopy_2008"]
merged["change_pct"] = 100 * merged["change"] / merged["canopy_2008"]
print(f"matched: {len(merged)} of {len(y2018)} neighbourhoods")

print(f"\nCitywide (area-weighted):")
print(f"  2008 canopy: {100 * merged.tree.sum() / merged.total_m2.sum():.2f}%")
print(f"  2018 canopy: {100 * (merged.canopy_2018/100 * merged.total_m2).sum() / merged.total_m2.sum():.2f}%")

# --- Findings ---
print("\n--- BIGGEST CANOPY LOSSES (percentage points) ---")
print(merged.nsmallest(15, "change")[["nbhd_name","canopy_2008","canopy_2018","change","classification"]].round(1).to_string(index=False))
print("\n--- BIGGEST CANOPY GAINS (percentage points) ---")
print(merged.nlargest(15, "change")[["nbhd_name","canopy_2008","canopy_2018","change","classification"]].round(1).to_string(index=False))

# --- NIA bucket ---
def bucket(c):
    c = str(c)
    if "Improvement" in c: return "NIA"
    if c.strip() == "Emerging Neighbourhood": return "Emerging"
    return "Not NIA/Emerging"


merged["bucket"] = merged["classification"].map(bucket)
print("\n--- avg change by NIA bucket ---")
print(merged.groupby("bucket").agg(
    n=("nbhd_name","count"),
    mean_change=("change","mean"),
    median_change=("change","median"),
    mean_2008=("canopy_2008","mean"),
    mean_2018=("canopy_2018","mean"),
).round(2).to_string())

# Save
merged[[
    "nbhd_name", "classification", "bucket",
    "canopy_2008", "canopy_2018", "change", "change_pct",
    "total_m2", "median_hh_income", "population_2021", "tree_count",
]].to_parquet(ROOT/"data/processed/nbhd_canopy_change.parquet", index=False)
print("\nwrote data/processed/nbhd_canopy_change.parquet")

# --- Charts ---
polys = con.execute(f"SELECT * FROM read_parquet('{ROOT/'data/processed/nbhd_polygons.parquet'}')").fetchdf()
polys["geom"] = polys["geom_wkb"].apply(lambda b: wkb.loads(bytes(b)))
polys["_k"] = polys.nbhd_name.map(slug)
gdf = merged.merge(polys[["_k","geom"]], on="_k", how="left")


def polys_to_patches(g):
    parts = g.geoms if g.geom_type == "MultiPolygon" else [g]
    return [MplPoly(np.array(p.exterior.coords), closed=True) for p in parts]


# 1) change choropleth (diverging)
fig, ax = plt.subplots(figsize=(11, 9))
values = gdf["change"].to_numpy()
vmax = max(abs(np.nanpercentile(values, 2)), abs(np.nanpercentile(values, 98)))
norm = plt.Normalize(vmin=-vmax, vmax=vmax)
cmap = plt.get_cmap("RdYlGn")
for i, row in gdf.iterrows():
    v = values[i]
    c = cmap(norm(v)) if np.isfinite(v) else (0.9,0.9,0.9,1)
    ax.add_collection(PatchCollection(polys_to_patches(row["geom"]),
                                      facecolor=c, edgecolor="white", linewidth=0.3))
ax.set_aspect("equal"); ax.autoscale_view(); ax.set_axis_off()
fig.suptitle("Change in tree canopy cover, 2008 → 2018", fontsize=17, weight="bold", x=0.05, ha="left", y=0.97)
fig.text(0.05, 0.93, "Percentage-point change per neighbourhood. Green gained, red lost.",
         fontsize=11, color="#666")
sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm); sm.set_array([])
cbar = fig.colorbar(sm, ax=ax, shrink=0.55, pad=0.02)
cbar.set_label("percentage-point change")
fig.tight_layout()
fig.savefig(CHARTS/"canopy_change_map.png", dpi=150, bbox_inches="tight")
plt.close(fig)
print(f"-> {CHARTS/'canopy_change_map.png'}")

# 2) Scatter: 2008 vs 2018 canopy with diagonal reference + income color
fig, ax = plt.subplots(figsize=(9, 8))

def cls_color(c):
    c = str(c)
    if "Improvement" in c: return "#d62728"
    if c.strip() == "Emerging Neighbourhood": return "#ff7f0e"
    return "#1f77b4"


colors = merged.classification.map(cls_color)
ax.plot([0,60],[0,60], color="#888", linestyle="--", lw=1, label="No change")
ax.scatter(merged.canopy_2008, merged.canopy_2018, c=colors, s=35, alpha=0.8,
           edgecolor="white", linewidth=0.4)

# Annotate biggest losers/gainers
for _, r in merged.nsmallest(5, "change").iterrows():
    ax.annotate(r.nbhd_name, (r.canopy_2008, r.canopy_2018), fontsize=8,
                xytext=(6, -8), textcoords="offset points", color="#b02020")
for _, r in merged.nlargest(4, "change").iterrows():
    ax.annotate(r.nbhd_name, (r.canopy_2008, r.canopy_2018), fontsize=8,
                xytext=(6, 6), textcoords="offset points", color="#1e5b2d")

ax.set_xlabel("Tree canopy %, 2008")
ax.set_ylabel("Tree canopy %, 2018")
ax.set_title("Ten years of canopy change, by neighbourhood", fontsize=13, weight="bold", loc="left")
ax.grid(alpha=0.3)
ax.set_xlim(0, 60); ax.set_ylim(0, 60)
fig.tight_layout()
fig.savefig(CHARTS/"canopy_change_scatter.png", dpi=150)
plt.close(fig)
print(f"-> {CHARTS/'canopy_change_scatter.png'}")

# 3) NIA bucket comparison
nb = merged.groupby("bucket")[["canopy_2008","canopy_2018"]].mean().reindex(["Not NIA/Emerging","Emerging","NIA"])
fig, ax = plt.subplots(figsize=(9, 5))
x = np.arange(len(nb))
w = 0.38
b1 = ax.bar(x - w/2, nb.canopy_2008, w, color="#6a9465", label="2008")
b2 = ax.bar(x + w/2, nb.canopy_2018, w, color="#2b7a3d", label="2018")
ax.set_xticks(x); ax.set_xticklabels(nb.index)
ax.set_ylabel("Mean tree canopy %")
ax.set_title("Canopy, 2008 vs 2018, by neighbourhood classification",
             fontsize=12, weight="bold", loc="left")
for b in list(b1) + list(b2):
    ax.annotate(f"{b.get_height():.1f}", (b.get_x()+b.get_width()/2, b.get_height()),
                ha="center", va="bottom", fontsize=9)
ax.legend(frameon=False)
ax.grid(alpha=0.3, axis="y")
ax.set_ylim(0, max(nb.values.max()*1.15, 35))
fig.tight_layout()
fig.savefig(CHARTS/"canopy_change_nia.png", dpi=150)
plt.close(fig)
print(f"-> {CHARTS/'canopy_change_nia.png'}")
