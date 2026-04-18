"""Map showing Toronto's 22 rarest tree species — each with an annotation.

All species with <=5 individuals citywide are shown as hot-pink dots over
a dim base of every other tree. Singletons are labelled.
"""
from pathlib import Path
import math
import duckdb
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "site" / "charts"
OUT.mkdir(parents=True, exist_ok=True)

BG = "#0e1114"
BASE = "#313539"
SINGLE = "#ffd93d"    # singletons — yellow (rarest of the rare)
FEW = "#ff6a87"       # 2-5 specimens — rose

con = duckdb.connect()
base = con.execute("""
    SELECT lon, lat FROM read_parquet('data/processed/trees.parquet')
    WHERE lat IS NOT NULL AND lon IS NOT NULL
""").fetchdf()
rare = pd.read_csv(ROOT / "data" / "processed" / "rare_trees.csv")
print(f"base: {len(base):,}  rare: {len(rare)}")

LAT_MID = (base.lat.min() + base.lat.max()) / 2
aspect = (base.lon.max() - base.lon.min()) * math.cos(math.radians(LAT_MID)) / (base.lat.max() - base.lat.min())
w = 22
fig, ax = plt.subplots(figsize=(w, w / aspect + 2.5), facecolor=BG)
ax.set_facecolor(BG)
ax.set_aspect(1 / math.cos(math.radians(LAT_MID)))

ax.scatter(base.lon, base.lat, c=BASE, s=0.8, alpha=0.35, linewidths=0)

singles = rare[rare.species_count == 1]
few = rare[rare.species_count > 1]
ax.scatter(few.lon, few.lat, c=FEW, s=40, alpha=0.85, linewidths=0, zorder=5)
ax.scatter(singles.lon, singles.lat, c=SINGLE, s=110, alpha=0.95, linewidths=0.5,
           edgecolors="white", zorder=10)

# Label every singleton with common name + address
for _, r in singles.iterrows():
    common = str(r.common_raw).split(",")[0].strip()
    label = f"{common}\n{int(r.ADDRESS)} {r.STREETNAME}"
    ax.annotate(
        label,
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

ax.text(0.015, 1.02, "Toronto's rarest street trees",
        transform=ax.transAxes, ha="left", va="bottom",
        fontsize=30, color="white", weight="bold")
ax.text(0.015, 1.005,
        "22 species with five or fewer specimens in the whole city.  "
        "Yellow = unique (one citywide). Rose = two to five.",
        transform=ax.transAxes, ha="left", va="bottom",
        fontsize=13, color="#b5b0a5")
ax.text(0.985, -0.01,
        "Data: City of Toronto Street Tree Data  ·  ttarabula.github.io/torontotrees",
        transform=ax.transAxes, ha="right", va="top",
        fontsize=10, color="#6a655a")

out = OUT / "rare_trees.png"
fig.savefig(out, dpi=180, facecolor=BG, bbox_inches="tight", pad_inches=0.3)
plt.close(fig)
print(f"-> {out} ({out.stat().st_size/1e6:.1f} MB)")
