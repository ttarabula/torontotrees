"""Hero chart for /blog/narrow-street-advantage/ — a Toronto map with
the 8 densest street-tree neighbourhoods (the prewar grid) highlighted
in green and the 8 sparsest (post-1960 developments) in red.
"""
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon as MplPoly
from matplotlib.lines import Line2D
from shapely import wkb
from shapely.geometry import MultiPolygon

ROOT = Path(__file__).resolve().parent.parent
PROC = ROOT / "data" / "processed"
OUT = ROOT / "site" / "charts" / "narrow_street_map.png"

LAT_MIN, LAT_MAX = 43.58, 43.86
LON_MIN, LON_MAX = -79.64, -79.12

BG = "#faf7f2"
DIM_STROKE = "#d9d4c7"
DIM_FILL = "#f3efe4"
DENSE = "#2b7a3d"
DENSE_EDGE = "#144c1c"
SPARSE = "#c43c3c"
SPARSE_EDGE = "#8a2323"


def geom_patches(geom):
    polys = geom.geoms if isinstance(geom, MultiPolygon) else [geom]
    return [MplPoly(list(p.exterior.coords), closed=True) for p in polys]


def main():
    con = duckdb.connect()
    nbhds = con.execute(f"""
        SELECT s.nbhd_code, s.nbhd_name, s.trees_per_km2, p.geom_wkb
        FROM read_parquet('{PROC}/nbhd_summary.parquet') s
        JOIN read_parquet('{PROC}/nbhd_polygons.parquet') p ON s.nbhd_code = p.nbhd_code
        ORDER BY s.trees_per_km2 DESC
    """).fetchdf()
    nbhds["geom"] = nbhds["geom_wkb"].apply(lambda b: wkb.loads(bytes(b)))

    top8 = nbhds.head(8)
    bot8 = nbhds.tail(8)

    fig, ax = plt.subplots(figsize=(9, 5.5), dpi=110)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    # All polygons — faint fill
    all_patches = []
    for _, row in nbhds.iterrows():
        all_patches.extend(geom_patches(row["geom"]))
    ax.add_collection(PatchCollection(
        all_patches, facecolor=DIM_FILL, edgecolor=DIM_STROKE, linewidths=0.4,
    ))

    # Densest 8 — green
    dense_patches = []
    for _, row in top8.iterrows():
        dense_patches.extend(geom_patches(row["geom"]))
    ax.add_collection(PatchCollection(
        dense_patches, facecolor=DENSE, alpha=0.85,
        edgecolor=DENSE_EDGE, linewidths=0.9,
    ))

    # Sparsest 8 — red
    sparse_patches = []
    for _, row in bot8.iterrows():
        sparse_patches.extend(geom_patches(row["geom"]))
    ax.add_collection(PatchCollection(
        sparse_patches, facecolor=SPARSE, alpha=0.85,
        edgecolor=SPARSE_EDGE, linewidths=0.9,
    ))

    ax.set_xlim(LON_MIN, LON_MAX)
    ax.set_ylim(LAT_MIN, LAT_MAX)
    ax.set_aspect(1 / 0.72)
    ax.axis("off")

    # Legend
    legend_elems = [
        Line2D([0], [0], marker="s", color="none", markerfacecolor=DENSE,
               markeredgecolor=DENSE_EDGE, markersize=12, label="8 densest (~9,000+ trees/km²)"),
        Line2D([0], [0], marker="s", color="none", markerfacecolor=SPARSE,
               markeredgecolor=SPARSE_EDGE, markersize=12, label="8 sparsest (~1,700 trees/km²)"),
    ]
    ax.legend(
        handles=legend_elems, loc="lower left", frameon=False,
        fontsize=10, handletextpad=0.6, labelspacing=0.4,
    )

    fig.tight_layout(pad=0.3)
    fig.savefig(OUT, facecolor=BG, bbox_inches="tight", pad_inches=0.08,
                pil_kwargs={"optimize": True})
    print(f"wrote {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
