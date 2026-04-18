"""Charts for the canopy blog post."""
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon as MplPoly
from matplotlib.lines import Line2D
from shapely import wkb

ROOT = Path(__file__).resolve().parent.parent
PROC = ROOT / "data" / "processed"
CHARTS = ROOT / "site" / "charts"

con = duckdb.connect()
df = con.execute(f"SELECT * FROM read_parquet('{PROC/'nbhd_canopy.parquet'}')").fetchdf()
polys = con.execute(f"SELECT * FROM read_parquet('{PROC/'nbhd_polygons.parquet'}')").fetchdf()
polys["geom"] = polys["geom_wkb"].apply(lambda b: wkb.loads(bytes(b)))

# Build nbhd_code → code via regex slug
import re
def slug(s): return re.sub(r"[^a-z0-9]", "", s.lower())
polys["_k"] = polys.nbhd_name.map(slug)
df["_k"] = df.nbhd_name.map(slug)
gdf = df.merge(polys[["_k","geom"]], on="_k", how="left")


def polys_to_patches(geom):
    parts = geom.geoms if geom.geom_type == "MultiPolygon" else [geom]
    return [MplPoly(np.array(p.exterior.coords), closed=True) for p in parts]


def choropleth(values, title, subtitle, cmap, outfile, label, vmin=None, vmax=None,
               label_nbhds=None):
    fig, ax = plt.subplots(figsize=(11, 9))
    cmap_obj = plt.get_cmap(cmap)
    vals = np.asarray(values, dtype=float)
    vmin = vmin if vmin is not None else np.nanpercentile(vals, 2)
    vmax = vmax if vmax is not None else np.nanpercentile(vals, 98)
    norm = plt.Normalize(vmin=vmin, vmax=vmax)
    for i, row in gdf.iterrows():
        v = vals[i]
        c = cmap_obj(norm(v)) if np.isfinite(v) else (0.9, 0.9, 0.9, 1)
        ax.add_collection(PatchCollection(polys_to_patches(row["geom"]),
                                          facecolor=c, edgecolor="white", linewidth=0.3))
    ax.set_aspect("equal"); ax.autoscale_view(); ax.set_axis_off()
    ax.set_title(title, fontsize=16, weight="bold", loc="left", pad=8)
    if subtitle:
        ax.text(0.0, 1.01, subtitle, transform=ax.transAxes,
                ha="left", va="bottom", fontsize=11, color="#666")
    if label_nbhds:
        for name in label_nbhds:
            row = gdf[gdf.nbhd_name == name]
            if len(row) == 0: continue
            g = row.iloc[0]["geom"]
            c = g.centroid
            v = vals[row.index[0]]
            ax.annotate(f"{name}\n{v:.0f}%", (c.x, c.y),
                        ha="center", va="center", fontsize=8.5, weight="bold",
                        color="white", bbox=dict(boxstyle="round,pad=0.3", fc="#000000cc", ec="none"))
    sm = plt.cm.ScalarMappable(cmap=cmap_obj, norm=norm); sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, shrink=0.55, pad=0.02)
    cbar.set_label(label)
    fig.tight_layout()
    fig.savefig(CHARTS / outfile, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"-> {CHARTS/outfile}")


# 1) Choropleth: canopy %
choropleth(
    df.canopy_pct.to_numpy(),
    "Toronto tree canopy cover, 2018",
    "Share of each neighbourhood covered by tree crowns, from LiDAR-derived land-cover data.",
    "YlGn", "map_canopy.png", "canopy %",
    vmin=0, vmax=55,
)


# 2) Scatter: canopy vs income (with NIA coloring)
def cls_color(c):
    c = str(c)
    if "Improvement" in c: return "#d62728"
    if c.strip() == "Emerging Neighbourhood": return "#ff7f0e"
    return "#1f77b4"


m = df.dropna(subset=["median_hh_income","canopy_pct"])
colors = m["classification"].map(cls_color)
fig, ax = plt.subplots(figsize=(9, 6))
ax.scatter(m.median_hh_income/1000, m.canopy_pct, c=colors, s=42, alpha=0.8,
           edgecolor="white", linewidth=0.5)
# annotate extremes
for _, r in m.nlargest(3, "canopy_pct").iterrows():
    ax.annotate(r.nbhd_name, (r.median_hh_income/1000, r.canopy_pct),
                fontsize=8, xytext=(5,-2), textcoords="offset points")
for _, r in m.nsmallest(3, "canopy_pct").iterrows():
    ax.annotate(r.nbhd_name, (r.median_hh_income/1000, r.canopy_pct),
                fontsize=8, xytext=(5,-2), textcoords="offset points")
r = m.canopy_pct.corr(m.median_hh_income)
ax.set_xlabel("Median household income, 2020 ($000s)")
ax.set_ylabel("Tree canopy cover, 2018 (%)")
ax.set_title(f"Canopy cover vs. household income (r = {r:.2f})")
ax.grid(alpha=0.3)
ax.legend(handles=[
    Line2D([0],[0], marker="o", color="w", markerfacecolor="#d62728", markersize=9, label="NIA"),
    Line2D([0],[0], marker="o", color="w", markerfacecolor="#ff7f0e", markersize=9, label="Emerging"),
    Line2D([0],[0], marker="o", color="w", markerfacecolor="#1f77b4", markersize=9, label="Other"),
], frameon=False, loc="upper left")
fig.tight_layout()
fig.savefig(CHARTS / "canopy_vs_income.png", dpi=150)
plt.close(fig)
print(f"-> {CHARTS/'canopy_vs_income.png'}")


# 3) Side-by-side: canopy equity vs street-tree equity
fig, (a1, a2) = plt.subplots(1, 2, figsize=(12, 5.5), sharex=True)
for ax, y, ycol, title, ylabel in [
    (a1, "canopy_pct", "canopy_pct", "Canopy cover vs. income", "Canopy %"),
    (a2, "trees_per_capita", "trees_per_capita", "Street trees per capita vs. income", "Street trees / person"),
]:
    m2 = df.dropna(subset=["median_hh_income", ycol])
    cc = m2.classification.map(cls_color)
    ax.scatter(m2.median_hh_income/1000, m2[ycol], c=cc, s=32, alpha=0.8, edgecolor="white", linewidth=0.4)
    r = m2[ycol].corr(m2.median_hh_income)
    ax.set_xlabel("Median household income ($000s)")
    ax.set_ylabel(ylabel)
    ax.set_title(f"{title}\nr = {r:.2f}", fontsize=11)
    ax.grid(alpha=0.3)
fig.suptitle("Two ways to measure Toronto's canopy equity", fontsize=13, weight="bold")
fig.tight_layout()
fig.savefig(CHARTS / "canopy_vs_streettree_equity.png", dpi=150)
plt.close(fig)
print(f"-> {CHARTS/'canopy_vs_streettree_equity.png'}")


# 4) Bar chart: NIA bucket comparison
def bucket(c):
    c = str(c)
    if "Improvement" in c: return "NIA"
    if c.strip() == "Emerging Neighbourhood": return "Emerging"
    return "Not NIA/Emerging"


df["bucket"] = df.classification.map(bucket)
g = df.groupby("bucket").agg(canopy=("canopy_pct","mean"),
                              per_capita=("trees_per_capita","mean"))
# normalize both to "non-NIA = 100" so we can compare gap sizes
ref_canopy = g.loc["Not NIA/Emerging", "canopy"]
ref_pc = g.loc["Not NIA/Emerging", "per_capita"]
g["canopy_rel"] = 100 * g.canopy / ref_canopy
g["pc_rel"] = 100 * g.per_capita / ref_pc
order = ["Not NIA/Emerging", "Emerging", "NIA"]
g = g.loc[order]

fig, ax = plt.subplots(figsize=(9, 5))
x = np.arange(len(g))
width = 0.38
bars1 = ax.bar(x - width/2, g.canopy_rel, width, color="#4a9860", label="Total canopy %")
bars2 = ax.bar(x + width/2, g.pc_rel, width, color="#d62728", label="Street trees per capita")
ax.axhline(100, color="#888", ls="--", lw=0.8)
ax.set_xticks(x)
ax.set_xticklabels(order)
ax.set_ylabel("Indexed to 'Not NIA/Emerging' = 100")
ax.set_title("The street-tree gap is bigger than the total-canopy gap",
             fontsize=12, weight="bold", loc="left")
ax.legend(frameon=False)
for b in bars1:
    ax.annotate(f"{b.get_height():.0f}", (b.get_x()+b.get_width()/2, b.get_height()),
                ha="center", va="bottom", fontsize=9)
for b in bars2:
    ax.annotate(f"{b.get_height():.0f}", (b.get_x()+b.get_width()/2, b.get_height()),
                ha="center", va="bottom", fontsize=9)
ax.set_ylim(0, 120)
ax.grid(alpha=0.3, axis="y")
fig.tight_layout()
fig.savefig(CHARTS / "canopy_nia_gap.png", dpi=150)
plt.close(fig)
print(f"-> {CHARTS/'canopy_nia_gap.png'}")
