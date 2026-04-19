"""Generate a small static PNG hero map for every neighbourhood page.

For each of Toronto's 158 neighbourhoods, renders:
  - All 158 polygons faintly outlined for context
  - A light dim-gray subsample of city-wide trees for texture
  - The target neighbourhood's polygon highlighted (accent-green)
  - Every street tree inside that neighbourhood as a bright green dot

Output: site/charts/nbhd/<slug>.png, ~20–40 KB each, matched to the
palette used elsewhere on the site.
"""
from pathlib import Path
import re

import duckdb
import matplotlib.pyplot as plt
from matplotlib.collections import PatchCollection
from matplotlib.patches import Polygon as MplPoly
from shapely import wkb
from shapely.geometry import MultiPolygon

ROOT = Path(__file__).resolve().parent.parent
PROC = ROOT / "data" / "processed"
OUT = ROOT / "site" / "charts" / "nbhd"
OUT.mkdir(parents=True, exist_ok=True)

# Toronto bbox — same as the canopy timeline so the maps crop identically.
LAT_MIN, LAT_MAX = 43.58, 43.86
LON_MIN, LON_MAX = -79.64, -79.12

BG = "#faf7f2"
DIM_STROKE = "#d9d4c7"
DIM_DOT = "#c8c0af"
ACCENT = "#2b7a3d"
ACCENT_FILL = "#e6efdf"   # very light — trees need to show through
ACCENT_DOT = "#1e5b2d"


def slug(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[.']", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s


def geom_patches(geom):
    """Turn a (Multi)Polygon into a list of matplotlib Polygon patches."""
    polys = geom.geoms if isinstance(geom, MultiPolygon) else [geom]
    patches = []
    for p in polys:
        patches.append(MplPoly(list(p.exterior.coords), closed=True))
    return patches


def main():
    con = duckdb.connect()

    nbhd_df = con.execute(f"""
        SELECT nbhd_code, nbhd_name, geom_wkb
        FROM read_parquet('{PROC}/nbhd_polygons.parquet')
    """).fetchdf()
    nbhd_df["geom"] = nbhd_df["geom_wkb"].apply(lambda b: wkb.loads(bytes(b)))

    # Subsample city-wide trees for the dim context layer.
    # 50K dots gives a good density visualization without file bloat.
    ctx_df = con.execute(f"""
        SELECT lat, lon, nbhd_code
        FROM read_parquet('{PROC}/trees.parquet')
        WHERE lat BETWEEN {LAT_MIN} AND {LAT_MAX}
          AND lon BETWEEN {LON_MIN} AND {LON_MAX}
        USING SAMPLE 50000
    """).fetchdf()

    # Per-nbhd trees (full, not sampled).
    trees_by_code = {}
    full_df = con.execute(f"""
        SELECT lat, lon, nbhd_code
        FROM read_parquet('{PROC}/trees.parquet')
        WHERE nbhd_code IS NOT NULL
          AND lat BETWEEN {LAT_MIN} AND {LAT_MAX}
          AND lon BETWEEN {LON_MIN} AND {LON_MAX}
    """).fetchdf()
    for code, grp in full_df.groupby("nbhd_code"):
        trees_by_code[code] = (grp["lon"].values, grp["lat"].values)

    # All polygons as patches, reused across every figure.
    all_patches = []
    for _, row in nbhd_df.iterrows():
        all_patches.extend(geom_patches(row["geom"]))

    for _, row in nbhd_df.iterrows():
        code = row["nbhd_code"]
        name = row["nbhd_name"]
        sl = slug(name)
        geom = row["geom"]

        # Zoom bbox: the nbhd's bounds with a 30% buffer on each side.
        minx, miny, maxx, maxy = geom.bounds
        dx, dy = maxx - minx, maxy - miny
        pad_x = max(dx * 0.25, 0.008)  # minimum ~800m buffer
        pad_y = max(dy * 0.25, 0.006)
        zx0, zx1 = minx - pad_x, maxx + pad_x
        zy0, zy1 = miny - pad_y, maxy + pad_y

        fig, ax = plt.subplots(figsize=(7, 4.4), dpi=95)
        fig.patch.set_facecolor(BG)
        ax.set_facecolor(BG)

        # Dim context trees in the zoom window
        zoom_ctx = (
            (ctx_df["lon"] >= zx0) & (ctx_df["lon"] <= zx1) &
            (ctx_df["lat"] >= zy0) & (ctx_df["lat"] <= zy1) &
            (ctx_df["nbhd_code"] != code)
        )
        # Full trees (not subsampled) for neighbouring nbhds in the zoom window,
        # so context is accurate at close range. Read a slice by bbox.
        nbr_df = full_df[
            (full_df["lon"] >= zx0) & (full_df["lon"] <= zx1) &
            (full_df["lat"] >= zy0) & (full_df["lat"] <= zy1) &
            (full_df["nbhd_code"] != code)
        ]
        ax.scatter(nbr_df["lon"], nbr_df["lat"], s=0.7, c=DIM_DOT, alpha=0.7, linewidths=0)

        # Other nbhd boundaries in the zoom window (dim)
        ax.add_collection(PatchCollection(
            all_patches, facecolor="none",
            edgecolor=DIM_STROKE, linewidths=0.5,
        ))

        # Highlighted polygon — very light fill so trees show through
        highlight_patches = geom_patches(geom)
        ax.add_collection(PatchCollection(
            highlight_patches, facecolor=ACCENT_FILL, alpha=0.55,
            edgecolor=ACCENT, linewidths=1.6,
        ))

        # This nbhd's trees — bigger, on top
        if code in trees_by_code:
            lons, lats = trees_by_code[code]
            ax.scatter(lons, lats, s=3.2, c=ACCENT_DOT, alpha=0.9, linewidths=0)

        ax.set_xlim(zx0, zx1)
        ax.set_ylim(zy0, zy1)
        ax.set_aspect(1 / 0.72)
        ax.axis("off")

        # Locator inset (top-right): whole city with the nbhd highlighted.
        ins = ax.inset_axes([0.72, 0.68, 0.28, 0.3])
        ins.set_facecolor("#ffffffdd")
        ins.add_collection(PatchCollection(
            all_patches, facecolor="#e8e3d5", edgecolor="#b3ac96",
            linewidths=0.3,
        ))
        ins.add_collection(PatchCollection(
            geom_patches(geom), facecolor=ACCENT, alpha=1.0, edgecolor="none",
        ))
        ins.set_xlim(LON_MIN, LON_MAX)
        ins.set_ylim(LAT_MIN, LAT_MAX)
        ins.set_aspect(1 / 0.72)
        ins.set_xticks([])
        ins.set_yticks([])
        for spine in ins.spines.values():
            spine.set_edgecolor("#8b8778")
            spine.set_linewidth(0.7)

        fig.tight_layout(pad=0)

        fig.savefig(
            OUT / f"{sl}.png",
            facecolor=BG, bbox_inches="tight", pad_inches=0.05,
            pil_kwargs={"optimize": True},
        )
        plt.close(fig)

    print(f"wrote {len(nbhd_df)} maps to {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
