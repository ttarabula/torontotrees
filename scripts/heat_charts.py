"""Charts for the heat-islands post."""
from pathlib import Path
import re
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


def slug(s): return re.sub(r"[^a-z0-9]", "", str(s).lower())


con = duckdb.connect()
df = con.execute(f"SELECT * FROM read_parquet('{PROC/'nbhd_heat.parquet'}')").fetchdf()
polys = con.execute(f"SELECT * FROM read_parquet('{PROC/'nbhd_polygons.parquet'}')").fetchdf()
polys["geom"] = polys["geom_wkb"].apply(lambda b: wkb.loads(bytes(b)))
polys["_k"] = polys.nbhd_name.map(slug)
df["_k"] = df.nbhd_name.map(slug)
gdf = df.merge(polys[["_k","geom"]], on="_k", how="left")


def polys_to_patches(g):
    parts = g.geoms if g.geom_type == "MultiPolygon" else [g]
    return [MplPoly(np.array(p.exterior.coords), closed=True) for p in parts]


# --- 1. Heat proxy choropleth ---
fig, ax = plt.subplots(figsize=(11, 9))
vals = gdf["heat_proxy"].to_numpy(dtype=float)
vmax = float(np.nanmax(np.abs(vals)))
norm = plt.Normalize(vmin=-vmax, vmax=vmax)
cmap = plt.get_cmap("RdYlGn_r")
for i, row in gdf.iterrows():
    v = vals[i]
    c = cmap(norm(v)) if np.isfinite(v) else (0.9,0.9,0.9,1)
    ax.add_collection(PatchCollection(polys_to_patches(row["geom"]),
                                      facecolor=c, edgecolor="white", linewidth=0.3))
ax.set_aspect("equal"); ax.autoscale_view(); ax.set_axis_off()
fig.suptitle("Toronto heat-risk proxy, by neighbourhood",
             fontsize=16, weight="bold", x=0.05, ha="left", y=0.97)
fig.text(0.05, 0.93,
         "Impervious % – tree-canopy %.  Red = hotter surface temperatures; green = cooler.",
         fontsize=10, color="#666")
sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm); sm.set_array([])
cbar = fig.colorbar(sm, ax=ax, shrink=0.55, pad=0.02)
cbar.set_label("heat proxy (pavement − canopy, %)")
fig.tight_layout()
fig.savefig(CHARTS/"heat_map.png", dpi=150, bbox_inches="tight")
plt.close(fig)
print(f"-> {CHARTS/'heat_map.png'}")


# --- 2. Heat vs income scatter ---
def cls_color(c):
    c = str(c)
    if "Improvement" in c: return "#d62728"
    if c.strip() == "Emerging Neighbourhood": return "#ff7f0e"
    return "#1f77b4"

m = df.dropna(subset=["median_hh_income","heat_proxy"])
colors = m["classification"].map(cls_color)
fig, ax = plt.subplots(figsize=(9, 6))
ax.scatter(m.median_hh_income/1000, m.heat_proxy, c=colors, s=42, alpha=0.8,
           edgecolor="white", linewidth=0.5)
for _, r in m.nlargest(4, "heat_proxy").iterrows():
    ax.annotate(r.nbhd_name, (r.median_hh_income/1000, r.heat_proxy),
                fontsize=8, xytext=(5,-4), textcoords="offset points", color="#a03030")
for _, r in m.nsmallest(4, "heat_proxy").iterrows():
    ax.annotate(r.nbhd_name, (r.median_hh_income/1000, r.heat_proxy),
                fontsize=8, xytext=(5,-4), textcoords="offset points", color="#1e5b2d")
rr = m.heat_proxy.corr(m.median_hh_income)
ax.axhline(0, color="#888", lw=0.8)
ax.set_xlabel("Median household income, 2020 ($000s)")
ax.set_ylabel("Heat proxy (pavement − canopy, %)")
ax.set_title(f"Heat proxy vs. household income (r = {rr:.2f})",
             fontsize=12, weight="bold", loc="left")
ax.grid(alpha=0.3)
ax.legend(handles=[
    Line2D([0],[0], marker="o", color="w", markerfacecolor="#d62728", markersize=9, label="NIA"),
    Line2D([0],[0], marker="o", color="w", markerfacecolor="#ff7f0e", markersize=9, label="Emerging"),
    Line2D([0],[0], marker="o", color="w", markerfacecolor="#1f77b4", markersize=9, label="Other"),
], frameon=False, loc="lower left")
fig.tight_layout()
fig.savefig(CHARTS/"heat_vs_income.png", dpi=150)
plt.close(fig)
print(f"-> {CHARTS/'heat_vs_income.png'}")


# --- 3. Canopy vs impervious scatter (colored by heat proxy) ---
fig, ax = plt.subplots(figsize=(9, 7))
cmap2 = plt.get_cmap("RdYlGn_r")
norm2 = plt.Normalize(vmin=-25, vmax=95)
sc = ax.scatter(df.canopy_pct, df.impervious_pct, c=df.heat_proxy, s=46,
                cmap=cmap2, norm=norm2, alpha=0.9, edgecolor="white", linewidth=0.5)
for _, r in df.nlargest(3, "heat_proxy").iterrows():
    ax.annotate(r.nbhd_name, (r.canopy_pct, r.impervious_pct), fontsize=8,
                xytext=(5,-4), textcoords="offset points", color="#a03030")
for _, r in df.nsmallest(3, "heat_proxy").iterrows():
    ax.annotate(r.nbhd_name, (r.canopy_pct, r.impervious_pct), fontsize=8,
                xytext=(5,-4), textcoords="offset points", color="#1e5b2d")
# iso-lines for constant heat proxy
for level in [-20, 0, 20, 40, 60, 80]:
    xs = np.linspace(0, 60, 2)
    ys = xs + level
    ax.plot(xs, ys, color="#aaa", lw=0.6, linestyle=":", alpha=0.6)
    # label
    x_anchor = 55
    y_anchor = x_anchor + level
    if 0 <= y_anchor <= 100:
        ax.text(x_anchor, y_anchor, f"  {level:+d}", fontsize=7, color="#888", va="center")
ax.set_xlabel("Tree canopy %")
ax.set_ylabel("Impervious surface % (building + road + paved + bare)")
ax.set_title("Heat proxy = impervious − canopy", fontsize=12, weight="bold", loc="left")
ax.grid(alpha=0.3)
ax.set_xlim(0, 60); ax.set_ylim(0, 100)
fig.colorbar(sc, ax=ax, shrink=0.6, label="heat proxy")
fig.tight_layout()
fig.savefig(CHARTS/"heat_canopy_vs_impervious.png", dpi=150)
plt.close(fig)
print(f"-> {CHARTS/'heat_canopy_vs_impervious.png'}")
