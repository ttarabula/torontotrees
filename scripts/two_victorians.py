"""Side-by-side comparison: Cabbagetown-South St.James Town vs Trinity-Bellwoods.

Outputs:
  site/charts/two_victorians_map.png     — side-by-side street-tree dot maps
  site/charts/two_victorians_landcover.png — stacked-bar land-cover breakdown
"""
from pathlib import Path
import math
import duckdb
import pyogrio
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon as MplPoly
from shapely import wkb
from shapely.geometry import Point

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "site" / "charts"
OUT.mkdir(parents=True, exist_ok=True)

CT = "Cabbagetown-South St.James Town"
TB = "Trinity-Bellwoods"
COLORS = {CT: "#4c8e3e", TB: "#c8623a"}

con = duckdb.connect()
# polygons
polys = con.execute(f"SELECT * FROM read_parquet('{ROOT/'data/processed/nbhd_polygons.parquet'}')").fetchdf()
polys["geom"] = polys["geom_wkb"].apply(lambda b: wkb.loads(bytes(b)))

# trees inside each
trees = con.execute(f"""
    SELECT nbhd_name, lat, lon, dbh_cm
    FROM read_parquet('{ROOT/'data/processed/trees.parquet'}')
    WHERE nbhd_name IN ('{CT}', '{TB}')
""").fetchdf()
print(trees.groupby("nbhd_name").size())


def polys_to_patches(geom):
    parts = geom.geoms if geom.geom_type == "MultiPolygon" else [geom]
    return [MplPoly(np.array(p.exterior.coords), closed=True) for p in parts]


# === 1) side-by-side maps ===
fig, axes = plt.subplots(1, 2, figsize=(15, 9), facecolor="#faf7f2")

# Use the larger bbox of the two so both panels are at the same scale
bboxes = {}
for name in [CT, TB]:
    g = polys[polys.nbhd_name == name].iloc[0]["geom"]
    bboxes[name] = g.bounds  # (minx, miny, maxx, maxy)

# Find max lon / lat span across both
max_lon_span = max(bx[2] - bx[0] for bx in bboxes.values())
max_lat_span = max(bx[3] - bx[1] for bx in bboxes.values())
pad_lon = max_lon_span * 0.06
pad_lat = max_lat_span * 0.06

for ax, name in zip(axes, [CT, TB]):
    ax.set_facecolor("#faf7f2")
    poly = polys[polys.nbhd_name == name].iloc[0]["geom"]
    # neighbourhood outline
    ax.add_collection(PatchCollection(polys_to_patches(poly),
                                      facecolor="#ece7dc", edgecolor="#8a8272", linewidth=1.2))
    # trees
    sub = trees[trees.nbhd_name == name]
    # size points by DBH (small 10-pt^2 for small, up to 80 for big)
    sizes = np.clip((sub.dbh_cm.fillna(10) / 3) ** 1.4, 6, 80)
    ax.scatter(sub.lon, sub.lat, s=sizes, c=COLORS[name], alpha=0.65,
               edgecolor="white", linewidth=0.25, zorder=5)
    # centre each panel on its own neighbourhood, but at common scale
    cx = (bboxes[name][0] + bboxes[name][2]) / 2
    cy = (bboxes[name][1] + bboxes[name][3]) / 2
    ax.set_xlim(cx - max_lon_span/2 - pad_lon, cx + max_lon_span/2 + pad_lon)
    ax.set_ylim(cy - max_lat_span/2 - pad_lat, cy + max_lat_span/2 + pad_lat)
    ax.set_aspect(1 / math.cos(math.radians(cy)))
    ax.set_axis_off()
    # stats panel under title
    n = len(sub)
    med_dbh = int(sub.dbh_cm.median())
    canopy_txt = {CT: "45%", TB: "20%"}[name]
    ax.text(0.02, 0.98,
            f"{name.replace(' St.James','\nSt.James')}" if "Cabbage" in name else name,
            transform=ax.transAxes, ha="left", va="top",
            fontsize=14, weight="bold", color="#1a1a1a")
    ax.text(0.02, 0.90,
            f"{n:,} street trees · median DBH {med_dbh} cm\n{canopy_txt} canopy cover",
            transform=ax.transAxes, ha="left", va="top",
            fontsize=11, color="#666")

fig.suptitle("A tale of two Victorians — Cabbagetown vs. Trinity-Bellwoods",
             fontsize=17, weight="bold", y=1.00)
fig.text(0.5, 0.01,
         "Each dot is one street tree; dot size ∝ trunk diameter. Both panels at identical geographic scale.",
         ha="center", fontsize=10, color="#666")
fig.tight_layout()
out = OUT / "two_victorians_map.png"
fig.savefig(out, dpi=160, bbox_inches="tight")
plt.close(fig)
print(f"-> {out}")


# === 2) stacked land-cover bars ===
lc = pyogrio.read_dataframe(str(ROOT/"data/raw/landcover/LandCover2018.gdb"),
                             layer="LandCover2018", read_geometry=False)

ORDER = ["tree", "shrub", "grass", "bare", "water", "road", "other", "building"]
COLOR_MAP = {
    "tree":     "#4a9860",
    "shrub":    "#8fb876",
    "grass":    "#c9dc9a",
    "bare":     "#c9a378",
    "water":    "#7db6d6",
    "road":     "#5a5a5a",
    "other":    "#8b8479",
    "building": "#2a2a2a",
}
LABEL = {
    "tree": "Tree canopy", "shrub": "Shrub", "grass": "Grass", "bare": "Bare",
    "water": "Water", "road": "Road", "other": "Paved (other)", "building": "Building",
}

fig, ax = plt.subplots(figsize=(11, 3.8))
fig.set_facecolor("#faf7f2")
ax.set_facecolor("#faf7f2")

y_labels = [CT, TB]
for i, name in enumerate(y_labels):
    sub = lc[lc.HoodName == name]
    agg = sub.groupby("Desc")["Shape_Area"].sum()
    total = agg.sum()
    left = 0.0
    for cls in ORDER:
        frac = agg.get(cls, 0) / total
        if frac <= 0:
            continue
        ax.barh(i, frac * 100, left=left, color=COLOR_MAP[cls], edgecolor="white", linewidth=0.6)
        if frac > 0.03:
            ax.text(left + frac * 50, i,
                    f"{LABEL[cls]}\n{100*frac:.0f}%",
                    ha="center", va="center", fontsize=9,
                    color="white" if cls in {"building","road","other","bare","tree"} else "#1a1a1a",
                    weight="bold" if cls == "tree" else "normal")
        left += frac * 100

ax.set_yticks(range(len(y_labels)))
ax.set_yticklabels([nm.replace(" St.James Town","\nSt.James Town") if "Cabbage" in nm else nm
                    for nm in y_labels], fontsize=10)
ax.set_xlim(0, 100)
ax.set_xlabel("Share of neighbourhood area (%)")
ax.set_title("Land-cover composition, 2018", loc="left", fontsize=14, weight="bold")
ax.spines[['top','right']].set_visible(False)
fig.tight_layout()
out = OUT / "two_victorians_landcover.png"
fig.savefig(out, dpi=160, bbox_inches="tight")
plt.close(fig)
print(f"-> {out}")
