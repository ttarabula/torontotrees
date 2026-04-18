"""Single-species Toronto map: all specimens plotted over a dim base of everything else.

Usage:  uv run scripts/species_map.py <genus> <species> <output_name> <color> "<title>" "<subtitle>"
"""
import sys
import math
from pathlib import Path
import duckdb
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "site" / "charts"
OUT.mkdir(parents=True, exist_ok=True)

BG = "#0e1114"
BASE = "#3a3d42"


def render(genus: str, species: str, outname: str, color: str,
           title: str, subtitle: str, highlight_big: int = 5):
    con = duckdb.connect()
    base = con.execute("""
        SELECT lon, lat FROM read_parquet('data/processed/trees.parquet')
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """).fetchdf()
    like = f"{genus.lower()} {species.lower()}"
    sub = con.execute(f"""
        SELECT lon, lat, dbh_cm, ADDRESS, STREETNAME, nbhd_name, botanical_raw
        FROM read_parquet('data/processed/trees.parquet')
        WHERE lower(botanical_raw) LIKE '{like}%'
          AND lat IS NOT NULL AND lon IS NOT NULL
    """).fetchdf()
    print(f"{genus} {species}: {len(sub):,} specimens")

    LAT_MID = (base.lat.min() + base.lat.max()) / 2
    aspect = (base.lon.max() - base.lon.min()) * math.cos(math.radians(LAT_MID)) / (base.lat.max() - base.lat.min())
    w = 22
    fig, ax = plt.subplots(figsize=(w, w / aspect + 2.5), facecolor=BG)
    ax.set_facecolor(BG)
    ax.set_aspect(1 / math.cos(math.radians(LAT_MID)))

    ax.scatter(base.lon, base.lat, c=BASE, s=0.8, alpha=0.3, linewidths=0)
    ax.scatter(sub.lon, sub.lat, c=color, s=5.0, alpha=0.8, linewidths=0)

    # Label the biggest DBH specimens
    big = sub[sub.dbh_cm.notna()].nlargest(highlight_big, "dbh_cm")
    for _, r in big.iterrows():
        ax.scatter(r.lon, r.lat, c=color, s=120, alpha=0.95, linewidths=1.5, edgecolor="white", zorder=10)
        ax.annotate(
            f"{int(r.dbh_cm)} cm DBH\n{int(r.ADDRESS)} {r.STREETNAME}",
            (r.lon, r.lat),
            xytext=(14, 14), textcoords="offset points",
            color="white", fontsize=10, weight="bold",
            ha="left", va="bottom",
            path_effects=[pe.withStroke(linewidth=3, foreground=BG)],
            arrowprops=dict(arrowstyle="-", color="#ffffffaa", lw=0.8, shrinkA=0, shrinkB=2),
        )

    ax.set_xlim(base.lon.min() - 0.005, base.lon.max() + 0.005)
    ax.set_ylim(base.lat.min() - 0.005, base.lat.max() + 0.005)
    ax.set_axis_off()
    ax.text(0.015, 1.02, title, transform=ax.transAxes, ha="left", va="bottom",
            fontsize=30, color="white", weight="bold")
    ax.text(0.015, 1.005, subtitle, transform=ax.transAxes, ha="left", va="bottom",
            fontsize=13, color="#b5b0a5")
    ax.text(0.985, -0.01,
            "Data: City of Toronto Street Tree Data  ·  ttarabula.github.io/torontotrees",
            transform=ax.transAxes, ha="right", va="top",
            fontsize=10, color="#6a655a")

    out = OUT / f"species_{outname}.png"
    fig.savefig(out, dpi=180, facecolor=BG, bbox_inches="tight", pad_inches=0.3)
    plt.close(fig)
    print(f"-> {out} ({out.stat().st_size/1e6:.1f} MB)")


if __name__ == "__main__":
    # default: black locust
    genus = sys.argv[1] if len(sys.argv) > 1 else "robinia"
    species = sys.argv[2] if len(sys.argv) > 2 else "pseudoacacia"
    outname = sys.argv[3] if len(sys.argv) > 3 else "blacklocust"
    color = sys.argv[4] if len(sys.argv) > 4 else "#b8d26f"
    title = sys.argv[5] if len(sys.argv) > 5 else "Toronto's black locusts"
    subtitle = sys.argv[6] if len(sys.argv) > 6 else "2,943 Robinia pseudoacacia in the street-tree inventory, labelled points are the five biggest"
    render(genus, species, outname, color, title, subtitle)
