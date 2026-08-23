"""Hero chart for /blog/the-61-centimetre-line/ — every Toronto neighbourhood
plotted by street-tree density (x) against the share of its street trees that
would clear the new bylaw's 61 cm "distinctive tree" threshold (y), coloured by
median household income.

The point of the chart is that the y-axis tracks the x-axis (old, dense grids
hold big trees) more tightly than it tracks the colour (income).
"""
from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LinearSegmentedColormap

ROOT = Path(__file__).resolve().parent.parent
PROC = ROOT / "data" / "processed"
OUT = ROOT / "site" / "charts" / "distinctive_vs_density.png"

DISTINCTIVE_CM = 61
MIN_TREES = 300

BG = "#faf7f2"
INK = "#1a1a1a"
MUTED = "#666666"
RULE = "#d9d4c7"

# Sequential single-hue ramp (light -> dark) for median household income.
INCOME_RAMP = LinearSegmentedColormap.from_list(
    "treeto_green", ["#dcebdd", "#a8cfae", "#6aae79", "#3d8b4f", "#1e5b2d"]
)

# Neighbourhoods called out by name in the post, with hand-tuned label offsets
# (in points) chosen to avoid collisions in the crowded 4k-8k density band.
LABELLED = {
    "Forest Hill North": (-14, 14),
    "Leaside-Bennington": (-96, 10),
    "The Beaches": (14, 8),
    "Morningside Heights": (16, 4),
    "Steeles": (14, -4),
    "Milliken": (14, 8),
}


def main():
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT s.nbhd_name,
               s.trees_per_km2,
               s.median_hh_income,
               COUNT(*)                                                    AS n,
               SUM(CASE WHEN t.dbh_cm >= {DISTINCTIVE_CM} THEN 1 ELSE 0 END) AS big
        FROM read_parquet('{PROC}/trees.parquet') t
        JOIN read_parquet('{PROC}/nbhd_summary.parquet') s
          ON t.nbhd_code = s.nbhd_code
        WHERE s.median_hh_income IS NOT NULL
        GROUP BY 1, 2, 3
        HAVING COUNT(*) >= {MIN_TREES}
        ORDER BY 1
    """).fetchdf()

    df["pct"] = df["big"] / df["n"] * 100

    r_density = np.corrcoef(df["pct"], df["trees_per_km2"])[0, 1]
    r_income = np.corrcoef(df["pct"], df["median_hh_income"])[0, 1]

    fig, ax = plt.subplots(figsize=(9, 5.6), dpi=110)
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(BG)

    sc = ax.scatter(
        df["trees_per_km2"], df["pct"],
        c=df["median_hh_income"], cmap=INCOME_RAMP,
        s=46, linewidths=0.8, edgecolors=BG, alpha=0.95, zorder=3,
    )

    for name, (dx, dy) in LABELLED.items():
        row = df[df["nbhd_name"] == name]
        if row.empty:
            continue
        x, y = float(row["trees_per_km2"].iloc[0]), float(row["pct"].iloc[0])
        # Ring the called-out points so the label's referent is unambiguous.
        ax.scatter([x], [y], s=92, facecolors="none", edgecolors=INK,
                   linewidths=1.1, zorder=4)
        ax.annotate(
            name, (x, y), textcoords="offset points", xytext=(dx, dy),
            fontsize=8.5, color=INK, zorder=5,
            arrowprops=dict(arrowstyle="-", color=MUTED, linewidth=0.7,
                            shrinkA=0, shrinkB=7),
        )

    ax.set_xlabel("City-owned street trees per km²", fontsize=9.5, color=MUTED)
    ax.set_ylabel(f"Share of street trees ≥ {DISTINCTIVE_CM} cm DBH", fontsize=9.5, color=MUTED)
    ax.yaxis.set_major_formatter(lambda v, _: f"{v:.0f}%")
    ax.tick_params(labelsize=8.5, colors=MUTED, length=0)
    ax.grid(True, color=RULE, linewidth=0.6, alpha=0.7, zorder=0)
    ax.set_axisbelow(True)
    for side in ("top", "right", "left", "bottom"):
        ax.spines[side].set_visible(False)

    ax.set_title(
        "What the 61 cm line selects for",
        fontsize=13, color=INK, fontweight="600", loc="left", pad=30,
    )
    ax.text(
        0, 1.012,
        f"158 Toronto neighbourhoods · density r = {r_density:.2f} · income r = {r_income:.2f}",
        transform=ax.transAxes, fontsize=9, color=MUTED, va="bottom",
    )

    cbar = fig.colorbar(sc, ax=ax, pad=0.015, fraction=0.035)
    cbar.set_label("Median household income", fontsize=8.5, color=MUTED)
    cbar.ax.tick_params(labelsize=7.5, colors=MUTED, length=0)
    cbar.ax.yaxis.set_major_formatter(lambda v, _: f"${v/1000:.0f}k")
    cbar.outline.set_visible(False)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT, facecolor=BG, bbox_inches="tight", pad_inches=0.12)
    print(f"wrote {OUT.relative_to(ROOT)}  (density r={r_density:.3f}, income r={r_income:.3f})")


if __name__ == "__main__":
    main()
