"""Generate findings charts.

Outputs to charts/:
  - income_vs_density.png      (equity scatter)
  - income_vs_percapita.png    (equity scatter, per-capita)
  - map_density.png            (choropleth: trees/km²)
  - map_shannon.png            (choropleth: Shannon H)
  - map_income.png             (choropleth: median household income)
  - top_species_bar.png        (city-wide top 15 species)
"""
from pathlib import Path
import duckdb
import matplotlib.pyplot as plt
import numpy as np
from shapely import wkb
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon as MplPoly
from matplotlib.colors import LinearSegmentedColormap

ROOT = Path(__file__).resolve().parent.parent
PROC = ROOT / "data" / "processed"
CHARTS = ROOT / "charts"
CHARTS.mkdir(exist_ok=True)

con = duckdb.connect()
summary = con.execute(f"SELECT * FROM read_parquet('{PROC/'nbhd_summary.parquet'}')").fetchdf()
polys = con.execute(f"SELECT * FROM read_parquet('{PROC/'nbhd_polygons.parquet'}')").fetchdf()
polys["geom"] = polys["geom_wkb"].apply(lambda b: wkb.loads(bytes(b)))
gdf = summary.merge(polys[["nbhd_code", "geom"]], on="nbhd_code", how="left")


def polys_to_patches(geom) -> list[MplPoly]:
    parts = geom.geoms if geom.geom_type == "MultiPolygon" else [geom]
    return [MplPoly(np.array(p.exterior.coords), closed=True) for p in parts]


def choropleth(values: np.ndarray, title: str, cmap: str, outfile: str,
               label: str, vmin: float | None = None, vmax: float | None = None) -> None:
    fig, ax = plt.subplots(figsize=(10, 10))
    cmap_obj = plt.get_cmap(cmap)
    vmin = vmin if vmin is not None else np.nanpercentile(values, 2)
    vmax = vmax if vmax is not None else np.nanpercentile(values, 98)
    norm = plt.Normalize(vmin=vmin, vmax=vmax)
    for _, row in gdf.iterrows():
        v = row_value(row, values, gdf)
        color = cmap_obj(norm(v)) if np.isfinite(v) else (0.9, 0.9, 0.9, 1)
        patches = polys_to_patches(row["geom"])
        pc = PatchCollection(patches, facecolor=color, edgecolor="white", linewidth=0.3)
        ax.add_collection(pc)
    ax.set_aspect("equal")
    ax.autoscale_view()
    ax.set_axis_off()
    ax.set_title(title, fontsize=14)
    sm = plt.cm.ScalarMappable(cmap=cmap_obj, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, shrink=0.5, pad=0.02)
    cbar.set_label(label)
    fig.tight_layout()
    fig.savefig(CHARTS / outfile, dpi=140, bbox_inches="tight")
    plt.close(fig)
    print(f"  -> {CHARTS / outfile}")


def row_value(row, values, df):
    return values[df.index.get_loc(row.name)]


# --- Scatter: income vs trees/km² ---
m = summary[summary["median_hh_income"].notna() & (summary["trees_per_km2"] > 0)]
fig, ax = plt.subplots(figsize=(9, 6))
def _cls_color(c: str) -> str:
    c = str(c)
    if "Improvement" in c:
        return "#d62728"
    if c.strip() == "Emerging Neighbourhood":
        return "#ff7f0e"
    return "#1f77b4"


colors = m["classification"].map(_cls_color)
ax.scatter(m["median_hh_income"] / 1000, m["trees_per_km2"],
           c=colors, alpha=0.75, s=40, edgecolor="white", linewidth=0.5)
# annotate notable
for _, r in m.nlargest(3, "trees_per_km2").iterrows():
    ax.annotate(r["nbhd_name"], (r["median_hh_income"]/1000, r["trees_per_km2"]),
                fontsize=8, xytext=(4, 4), textcoords="offset points")
for _, r in m.nsmallest(3, "trees_per_km2").iterrows():
    ax.annotate(r["nbhd_name"], (r["median_hh_income"]/1000, r["trees_per_km2"]),
                fontsize=8, xytext=(4, 4), textcoords="offset points")
r = m["median_hh_income"].corr(m["trees_per_km2"])
ax.set_xlabel("Median household income, 2020 ($000s)")
ax.set_ylabel("Street trees per km² (city-owned, road allowance)")
ax.set_title(f"Toronto street-tree density vs household income (r = {r:.2f})")
ax.grid(alpha=0.25)
# legend
from matplotlib.lines import Line2D
ax.legend(handles=[
    Line2D([0], [0], marker="o", color="w", markerfacecolor="#d62728", markersize=9, label="Improvement Area"),
    Line2D([0], [0], marker="o", color="w", markerfacecolor="#ff7f0e", markersize=9, label="Emerging"),
    Line2D([0], [0], marker="o", color="w", markerfacecolor="#1f77b4", markersize=9, label="Other"),
], loc="lower right", frameon=False)
fig.tight_layout()
fig.savefig(CHARTS / "income_vs_density.png", dpi=140)
plt.close(fig)
print(f"  -> {CHARTS / 'income_vs_density.png'}")

# --- Scatter: income vs trees/capita ---
m2 = summary[summary["median_hh_income"].notna() & (summary["population_2021"] > 0)]
fig, ax = plt.subplots(figsize=(9, 6))
colors2 = m2["classification"].map(_cls_color)
ax.scatter(m2["median_hh_income"] / 1000, m2["trees_per_capita"],
           c=colors2, alpha=0.75, s=40, edgecolor="white", linewidth=0.5)
r2 = m2["median_hh_income"].corr(m2["trees_per_capita"])
ax.set_xlabel("Median household income, 2020 ($000s)")
ax.set_ylabel("Street trees per capita")
ax.set_title(f"Toronto street trees per capita vs household income (r = {r2:.2f})")
ax.grid(alpha=0.25)
fig.tight_layout()
fig.savefig(CHARTS / "income_vs_percapita.png", dpi=140)
plt.close(fig)
print(f"  -> {CHARTS / 'income_vs_percapita.png'}")

# --- Maps ---
choropleth(
    summary["trees_per_km2"].to_numpy(),
    "Toronto street-tree density by neighbourhood",
    "YlGn", "map_density.png", "trees / km²",
)
choropleth(
    summary["shannon_h"].to_numpy(),
    "Street-tree species diversity (Shannon H)",
    "viridis", "map_shannon.png", "Shannon H",
)
choropleth(
    summary["median_hh_income"].to_numpy(),
    "Median household income, 2020",
    "plasma", "map_income.png", "$",
)

# --- top species bar ---
top = con.execute(f"""
    SELECT botanical_key, COUNT(*) n
    FROM read_parquet('{PROC/'trees.parquet'}')
    GROUP BY 1 ORDER BY n DESC LIMIT 15
""").fetchdf()
fig, ax = plt.subplots(figsize=(9, 6))
ax.barh(top["botanical_key"][::-1], top["n"][::-1], color="#2ca02c")
ax.set_xlabel("tree count")
ax.set_title("Top 15 species in Toronto street-tree inventory")
for i, (b, n) in enumerate(zip(top["botanical_key"][::-1], top["n"][::-1])):
    ax.text(n, i, f"  {n:,}", va="center", fontsize=8)
fig.tight_layout()
fig.savefig(CHARTS / "top_species_bar.png", dpi=140)
plt.close(fig)
print(f"  -> {CHARTS / 'top_species_bar.png'}")
