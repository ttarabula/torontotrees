"""Map the single biggest living specimen of each of Toronto's top 25 species.

Outputs:
  site/charts/veterans_map.png
  data/processed/veterans.csv — used to populate the blog post table
"""
from pathlib import Path
import math
import duckdb
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe

ROOT = Path(__file__).resolve().parent.parent
CHARTS = ROOT / "site" / "charts"
CHARTS.mkdir(parents=True, exist_ok=True)

BG = "#0e1114"
BASE = "#323539"
GOLD = "#e6c24a"


con = duckdb.connect()
con.execute("""
CREATE VIEW t AS
SELECT *,
  regexp_extract(
    trim(regexp_replace(lower(botanical_raw), '''[^'']*''', '', 'g')),
    '^([a-z-]+[[:space:]]+[a-z-]+)', 1
  ) AS k
FROM read_parquet('data/processed/trees.parquet')
WHERE dbh_cm IS NOT NULL AND dbh_cm < 250 AND botanical_raw IS NOT NULL
""")
veterans = con.execute("""
WITH top_species AS (
  SELECT k, COUNT(*) AS n FROM t WHERE k <> '' GROUP BY 1 ORDER BY n DESC LIMIT 25
),
ranked AS (
  SELECT t.k, t.common_raw, t.botanical_raw, t.dbh_cm, t.ADDRESS, t.STREETNAME,
         t.nbhd_name, t.lat, t.lon, ts.n AS species_count,
         ROW_NUMBER() OVER (PARTITION BY t.k ORDER BY t.dbh_cm DESC, t._id) AS rn
  FROM t JOIN top_species ts USING (k)
)
SELECT * FROM ranked WHERE rn = 1 ORDER BY dbh_cm DESC
""").fetchdf()
print(veterans[["k","common_raw","dbh_cm","ADDRESS","STREETNAME","nbhd_name"]].head(5).to_string(index=False))

veterans.to_csv(ROOT / "data" / "processed" / "veterans.csv", index=False)

# Base layer: all trees
base = con.execute("""
    SELECT lat, lon FROM read_parquet('data/processed/trees.parquet')
    WHERE lat IS NOT NULL AND lon IS NOT NULL
""").fetchdf()

LAT_MID = (base.lat.min() + base.lat.max()) / 2
aspect = (base.lon.max() - base.lon.min()) * math.cos(math.radians(LAT_MID)) / (base.lat.max() - base.lat.min())
w = 22
fig, ax = plt.subplots(figsize=(w, w / aspect + 2.5), facecolor=BG)
ax.set_facecolor(BG)
ax.set_aspect(1 / math.cos(math.radians(LAT_MID)))

ax.scatter(base.lon, base.lat, c=BASE, s=0.55, alpha=0.3, linewidths=0)

for _, r in veterans.iterrows():
    ax.scatter(r.lon, r.lat, c=GOLD, s=min(r.dbh_cm * 1.1, 280), alpha=0.95,
               linewidths=1.5, edgecolor="white", zorder=5)

# Labels for top 8 biggest, to avoid clutter
for _, r in veterans.head(8).iterrows():
    label = f"{r.common_raw}\n{int(r.dbh_cm)} cm  —  {int(r.ADDRESS)} {r.STREETNAME}"
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

ax.text(0.015, 1.02, "Toronto's veteran street trees",
        transform=ax.transAxes, ha="left", va="bottom",
        fontsize=30, color="white", weight="bold")
ax.text(0.015, 1.005,
        "The single largest specimen (by trunk diameter) of each of the top 25 species — "
        "the giants among 689,000 inventory trees.",
        transform=ax.transAxes, ha="left", va="bottom",
        fontsize=13, color="#b5b0a5")
ax.text(0.985, -0.01,
        "Data: City of Toronto Street Tree Data  ·  treeto.ca",
        transform=ax.transAxes, ha="right", va="top",
        fontsize=10, color="#6a655a")

out = CHARTS / "veterans_map.png"
fig.savefig(out, dpi=180, facecolor=BG, bbox_inches="tight", pad_inches=0.3)
plt.close(fig)
print(f"-> {out}")
