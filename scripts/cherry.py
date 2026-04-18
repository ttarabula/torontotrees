"""Cherry blossom maps for the spring-pilgrimage blog post.

Outputs:
  site/charts/cherry_citywide.png   — all 7,277 showy Prunus as pink dots over Toronto
  site/charts/cherry_oakwood.png    — zoom into the Oakwood Village / west-end cluster
  site/charts/cherry_highpark.png   — zoom into the Colborne Lodge Dr / High Park grove
Also prints ranked lists for the blog post.
"""
from pathlib import Path
import math
import duckdb
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "site" / "charts"
OUT.mkdir(parents=True, exist_ok=True)

BG = "#14110e"       # dark warm brown
BASE_DOT = "#3c362e" # muted warm gray for non-cherry trees
CHERRY = "#ff80b5"   # soft sakura pink
CHERRY_STRONG = "#ff5099"

SHOWY = """
    lower(botanical_raw) LIKE 'prunus serrulata%'
    OR lower(botanical_raw) LIKE 'prunus %yedoensis%'
    OR lower(botanical_raw) LIKE 'prunus sargentii%'
    OR lower(botanical_raw) LIKE 'prunus subhirtella%'
    OR lower(botanical_raw) LIKE 'prunus triloba%'
    OR lower(botanical_raw) LIKE 'prunus persica%'
    OR lower(botanical_raw) LIKE 'prunus armeniaca%'
    OR (lower(botanical_raw) = 'prunus spp.' AND common_raw IN ('Cherry', 'Plum'))
"""

con = duckdb.connect()
base = con.execute("""
    SELECT lon, lat FROM read_parquet('data/processed/trees.parquet')
    WHERE lat IS NOT NULL AND lon IS NOT NULL
""").fetchdf()
cherries = con.execute(f"""
    SELECT lon, lat, botanical_raw, common_raw, STREETNAME, nbhd_name, ADDRESS
    FROM read_parquet('data/processed/trees.parquet')
    WHERE ({SHOWY})
      AND lat IS NOT NULL AND lon IS NOT NULL
""").fetchdf()
print(f"base: {len(base):,}  cherries: {len(cherries):,}")


def set_map_aspect(ax, lat_mid):
    ax.set_aspect(1 / math.cos(math.radians(lat_mid)))
    ax.set_axis_off()


# === 1. CITYWIDE ===
LAT_MID = (base.lat.min() + base.lat.max()) / 2
aspect = (base.lon.max() - base.lon.min()) * math.cos(math.radians(LAT_MID)) / (base.lat.max() - base.lat.min())
w = 22
fig, ax = plt.subplots(figsize=(w, w / aspect + 2), facecolor=BG)
ax.set_facecolor(BG)
ax.scatter(base.lon, base.lat, c=BASE_DOT, s=1.0, alpha=0.35, linewidths=0)
ax.scatter(cherries.lon, cherries.lat, c=CHERRY, s=4.0, alpha=0.85, linewidths=0)
ax.set_xlim(base.lon.min() - 0.005, base.lon.max() + 0.005)
ax.set_ylim(base.lat.min() - 0.005, base.lat.max() + 0.005)
set_map_aspect(ax, LAT_MID)
ax.text(0.015, 1.02, "Where to see cherry blossoms in Toronto",
        transform=ax.transAxes, ha="left", va="bottom",
        fontsize=32, color="white", weight="bold")
ax.text(0.015, 1.005,
        "7,277 flowering cherries, plums, peaches and apricots on city-owned road allowances.  "
        "Gray = every other street tree.",
        transform=ax.transAxes, ha="left", va="bottom",
        fontsize=14, color="#c2b5a6")
ax.text(0.985, -0.01,
        "Data: City of Toronto Street Tree Data  ·  ttarabula.github.io/torontotrees",
        transform=ax.transAxes, ha="right", va="top",
        fontsize=10, color="#7a6e5f")
out = OUT / "cherry_citywide.png"
fig.savefig(out, dpi=180, facecolor=BG, bbox_inches="tight", pad_inches=0.3)
plt.close(fig)
print(f"-> {out} ({out.stat().st_size/1e6:.1f} MB)")


# === 2. OAKWOOD / WEST-END CLUSTER ZOOM ===
def zoom_map(outfile, title, subtitle, lon_min, lon_max, lat_min, lat_max, label_streets=None):
    lat_mid = (lat_min + lat_max) / 2
    asp = (lon_max - lon_min) * math.cos(math.radians(lat_mid)) / (lat_max - lat_min)
    w = 14
    fig, ax = plt.subplots(figsize=(w, w / asp + 1.8), facecolor=BG)
    ax.set_facecolor(BG)
    # clip both datasets to zoom window
    def clip(df):
        return df[(df.lon >= lon_min) & (df.lon <= lon_max) & (df.lat >= lat_min) & (df.lat <= lat_max)]
    b = clip(base)
    c = clip(cherries)
    ax.scatter(b.lon, b.lat, c=BASE_DOT, s=4.0, alpha=0.5, linewidths=0)
    ax.scatter(c.lon, c.lat, c=CHERRY_STRONG, s=28, alpha=0.85, linewidths=0,
               edgecolors="none")
    if label_streets:
        for street, tx_shift, ty_shift in label_streets:
            sub = c[c.STREETNAME == street]
            if len(sub) == 0:
                continue
            lat_c = sub.lat.mean()
            lon_c = sub.lon.mean()
            ax.annotate(f"{street}  ({len(sub)})",
                        (lon_c, lat_c),
                        xytext=(tx_shift, ty_shift), textcoords="offset points",
                        color="white", fontsize=11, weight="bold",
                        ha="left", va="center",
                        path_effects=[pe.withStroke(linewidth=3, foreground=BG)],
                        arrowprops=dict(arrowstyle="-", color="#ffffff88", lw=0.8))
    ax.set_xlim(lon_min, lon_max)
    ax.set_ylim(lat_min, lat_max)
    set_map_aspect(ax, lat_mid)
    ax.text(0.015, 1.02, title, transform=ax.transAxes, ha="left", va="bottom",
            fontsize=22, color="white", weight="bold")
    ax.text(0.015, 1.005, subtitle, transform=ax.transAxes, ha="left", va="bottom",
            fontsize=12, color="#c2b5a6")
    fig.savefig(outfile, dpi=180, facecolor=BG, bbox_inches="tight", pad_inches=0.25)
    plt.close(fig)
    print(f"-> {outfile}")


# Oakwood / west-end belt: roughly between St Clair to Eglinton, Dufferin to Jane
zoom_map(
    OUT / "cherry_oakwood.png",
    "The west-end cherry belt",
    "Oakwood Village, Corso Italia, Caledonia-Fairbank — denser with flowering Prunus than High Park.",
    lon_min=-79.48, lon_max=-79.41, lat_min=43.67, lat_max=43.71,
    label_streets=[
        ("LAUDER AVE", 20, 10),
        ("NORTHCLIFFE BLVD", 20, -15),
        ("GLENHOLME AVE", 25, 0),
        ("BOWIE AVE", -110, -5),
        ("SYMINGTON AVE", -120, 10),
    ],
)

# High Park grove zoom
zoom_map(
    OUT / "cherry_highpark.png",
    "High Park & Parkside",
    "Colborne Lodge Dr hosts 87 Japanese cherries (Prunus serrulata) — the city's best-known sakura grove.",
    lon_min=-79.475, lon_max=-79.44, lat_min=43.636, lat_max=43.660,
    label_streets=[
        ("COLBORNE LODGE DR", 20, 0),
        ("PARKSIDE DR", 25, 0),
    ],
)
