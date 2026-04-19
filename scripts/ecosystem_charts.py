"""Charts for the value-of-the-canopy post."""
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon as MplPoly
from matplotlib.lines import Line2D
from shapely import wkb
import re

ROOT = Path(__file__).resolve().parent.parent
PROC = ROOT / "data" / "processed"
CHARTS = ROOT / "site" / "charts"


def slug(s): return re.sub(r"[^a-z0-9]", "", str(s).lower())


con = duckdb.connect()
df = con.execute(f"SELECT * FROM read_parquet('{PROC/'nbhd_value.parquet'}')").fetchdf()
polys = con.execute(f"SELECT * FROM read_parquet('{PROC/'nbhd_polygons.parquet'}')").fetchdf()
polys["geom"] = polys["geom_wkb"].apply(lambda b: wkb.loads(bytes(b)))
polys["_k"] = polys.nbhd_name.map(slug)
df["_k"] = df.nbhd_name.map(slug)
gdf = df.merge(polys[["_k","geom"]], on="_k", how="left")


def polys_to_patches(g):
    parts = g.geoms if g.geom_type == "MultiPolygon" else [g]
    return [MplPoly(np.array(p.exterior.coords), closed=True) for p in parts]


# ------ 1. Choropleth: per-capita USD ------
fig, ax = plt.subplots(figsize=(11, 9))
cmap = plt.get_cmap("YlGn")
vals = gdf.usd_per_capita.to_numpy(dtype=float)
vmin, vmax = 0, np.nanpercentile(vals, 98)
norm = plt.Normalize(vmin=vmin, vmax=vmax)
for i, row in gdf.iterrows():
    v = vals[i]
    c = cmap(norm(v)) if np.isfinite(v) else (0.9, 0.9, 0.9, 1)
    ax.add_collection(PatchCollection(polys_to_patches(row["geom"]),
                                      facecolor=c, edgecolor="white", linewidth=0.3))
ax.set_aspect("equal"); ax.autoscale_view(); ax.set_axis_off()
fig.suptitle("Annual ecosystem-service value of street trees, per resident",
             fontsize=15, weight="bold", x=0.05, ha="left", y=0.97)
fig.text(0.05, 0.93,
         "Estimated USD/year per resident from city-owned street trees in each neighbourhood. "
         "See methodology note for caveats.",
         fontsize=10, color="#666")
sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm); sm.set_array([])
cbar = fig.colorbar(sm, ax=ax, shrink=0.55, pad=0.02)
cbar.set_label("USD / resident / year")
fig.tight_layout()
fig.savefig(CHARTS/"value_map.png", dpi=150, bbox_inches="tight")
plt.close(fig)
print(f"-> {CHARTS/'value_map.png'}")

# ------ 2. Scatter: per-capita value vs income ------
def cls_color(c):
    c = str(c)
    if "Improvement" in c: return "#d62728"
    if c.strip() == "Emerging Neighbourhood": return "#ff7f0e"
    return "#1f77b4"

m = df.dropna(subset=["median_hh_income","usd_per_capita"])
colors = m["classification"].map(cls_color)
fig, ax = plt.subplots(figsize=(9, 6))
ax.scatter(m.median_hh_income/1000, m.usd_per_capita, c=colors, s=42, alpha=0.8,
           edgecolor="white", linewidth=0.5)
for _, r in m.nlargest(3, "usd_per_capita").iterrows():
    ax.annotate(r.nbhd_name, (r.median_hh_income/1000, r.usd_per_capita),
                fontsize=8, xytext=(5,-4), textcoords="offset points")
for _, r in m.nsmallest(3, "usd_per_capita").iterrows():
    ax.annotate(r.nbhd_name, (r.median_hh_income/1000, r.usd_per_capita),
                fontsize=8, xytext=(5,-4), textcoords="offset points")
rr = m.usd_per_capita.corr(m.median_hh_income)
ax.set_xlabel("Median household income, 2020 ($000s)")
ax.set_ylabel("Street-tree ecosystem value per resident (USD/yr)")
ax.set_title(f"Per-capita ecosystem-service value vs. income (r = {rr:.2f})",
             fontsize=12, weight="bold", loc="left")
ax.grid(alpha=0.3)
ax.legend(handles=[
    Line2D([0],[0], marker="o", color="w", markerfacecolor="#d62728", markersize=9, label="NIA"),
    Line2D([0],[0], marker="o", color="w", markerfacecolor="#ff7f0e", markersize=9, label="Emerging"),
    Line2D([0],[0], marker="o", color="w", markerfacecolor="#1f77b4", markersize=9, label="Other"),
], frameon=False, loc="upper left")
fig.tight_layout()
fig.savefig(CHARTS/"value_vs_income.png", dpi=150)
plt.close(fig)
print(f"-> {CHARTS/'value_vs_income.png'}")

# ------ 3. Top species by total annual value ------
tv = con.execute(f"SELECT * FROM read_parquet('{PROC/'tree_value.parquet'}')").fetchdf()
sp = tv.groupby("common_raw").agg(n=("_id","count"), total=("annual_usd","sum"))
top = sp.sort_values("total", ascending=False).head(12)[::-1]
fig, ax = plt.subplots(figsize=(9, 6))
bars = ax.barh(top.index, top["total"]/1e6, color="#4a9860")
for b, n in zip(bars, top["n"]):
    ax.text(b.get_width(), b.get_y()+b.get_height()/2,
            f"  ${b.get_width():.2f}M  ({int(n):,} trees)",
            va="center", fontsize=8)
ax.set_xlabel("Annual ecosystem-service value (USD millions)")
ax.set_title("Top 12 species by total annual ecosystem-service value",
             fontsize=12, weight="bold", loc="left")
ax.set_xlim(0, top["total"].max()/1e6 * 1.35)
fig.tight_layout()
fig.savefig(CHARTS/"value_top_species.png", dpi=150)
plt.close(fig)
print(f"-> {CHARTS/'value_top_species.png'}")
