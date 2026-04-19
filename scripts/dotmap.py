"""Every tree in Toronto, as a dot — coloured by genus.

Produces three views:
  dotmap_dark.png        — all 689k trees coloured by genus on dark bg
  dotmap_norway.png      — Norway maple (the invasive #1) vs. everything else
  dotmap_native.png      — native vs. introduced vs. unknown
"""
from pathlib import Path
import json
import duckdb
import matplotlib.pyplot as plt
import matplotlib.patheffects as pe
from matplotlib.lines import Line2D

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "site" / "charts"
OUT.mkdir(parents=True, exist_ok=True)

GENUS_COLORS = {
    "acer":        "#c43c3c",  # maple -> red
    "gleditsia":   "#e3b14d",  # honey locust -> warm gold
    "quercus":     "#b38851",  # oak -> bronze
    "tilia":       "#8ec76b",  # linden -> light green
    "picea":       "#4aa894",  # spruce -> blue-green
    "ulmus":       "#3d8fb8",  # elm -> teal
    "gymnocladus": "#9e68c8",  # Kentucky coffeetree -> purple
    "malus":       "#f49bc4",  # crabapple -> pink
    "syringa":     "#c390d6",  # lilac -> lavender
    "pinus":       "#4d9769",  # pine -> forest green
    "ginkgo":      "#f0d24a",  # ginkgo -> vivid yellow
    "celtis":      "#a17a52",  # hackberry -> earthy brown
    "betula":      "#f0ebdb",  # birch -> cream
    "amelanchier": "#d67daa",  # serviceberry -> mauve
    "aesculus":    "#f08c3a",  # horsechestnut -> orange
}
OTHER_COLOR = "#6a6a6a"
BG_DARK = "#0b0d10"

con = duckdb.connect()
df = con.execute("""
    SELECT lon, lat, botanical_key,
           split_part(botanical_key, ' ', 1) AS genus
    FROM read_parquet('data/processed/trees.parquet')
    WHERE lat IS NOT NULL AND lon IS NOT NULL
""").fetchdf()

# Match Toronto's true aspect ratio (lon span / lat span at Toronto's latitude)
LAT_MID = (df.lat.min() + df.lat.max()) / 2
import math
aspect = (df.lon.max() - df.lon.min()) * math.cos(math.radians(LAT_MID)) / (df.lat.max() - df.lat.min())
print(f"Toronto lat/lon aspect: {aspect:.2f}")


def figure_for_map(w=22, title_color="white", bg=BG_DARK):
    h = w / aspect + 2.5  # extra room for title/legend
    fig, ax = plt.subplots(figsize=(w, h), facecolor=bg)
    ax.set_facecolor(bg)
    ax.set_aspect(1 / math.cos(math.radians(LAT_MID)))
    pad_lon = (df.lon.max() - df.lon.min()) * 0.01
    pad_lat = (df.lat.max() - df.lat.min()) * 0.01
    ax.set_xlim(df.lon.min() - pad_lon, df.lon.max() + pad_lon)
    ax.set_ylim(df.lat.min() - pad_lat, df.lat.max() + pad_lat)
    ax.set_axis_off()
    return fig, ax


def add_titles(ax, title, subtitle, bg=BG_DARK):
    # Above-map titles (use data coords relative to axes bbox)
    ax.text(0.015, 1.02, title, transform=ax.transAxes,
            ha="left", va="bottom", fontsize=32, color="white", weight="bold")
    ax.text(0.015, 1.005, subtitle, transform=ax.transAxes,
            ha="left", va="bottom", fontsize=14, color="#a8a39a")
    ax.text(0.985, -0.01, "Data: City of Toronto Street Tree Data (OGL-Toronto)  ·  treeto.ca",
            transform=ax.transAxes, ha="right", va="top", fontsize=10, color="#6a655a")


# ============================================================
# 1) GENUS MAP
# ============================================================
df["color"] = df["genus"].map(GENUS_COLORS).fillna(OTHER_COLOR)
df["known"] = df["genus"].isin(GENUS_COLORS)
print(f"known-genus: {df.known.sum():,}  other: {(~df.known).sum():,}")

fig, ax = figure_for_map(w=24)
other = df[~df.known]
known = df[df.known]
# Other in a muted gray behind
ax.scatter(other.lon, other.lat, c=other.color, s=1.2, alpha=0.4, linewidths=0)
# Known on top
ax.scatter(known.lon, known.lat, c=known.color, s=1.4, alpha=0.75, linewidths=0)

add_titles(ax, "Every street tree in Toronto",
           f"{len(df):,} city-owned trees on road allowances, coloured by genus")

# Legend in two columns, placed inside lower-left with transparent bg
counts = df.genus.value_counts()
handles = []
for genus, color in GENUS_COLORS.items():
    n = int(counts.get(genus, 0))
    handles.append(Line2D([0], [0], marker="o", color="none",
                          markerfacecolor=color, markeredgecolor="none",
                          markersize=10, label=f"{genus.title():<14}  {n:>7,}"))
handles.append(Line2D([0], [0], marker="o", color="none",
                      markerfacecolor=OTHER_COLOR, markeredgecolor="none",
                      markersize=10, label=f"{'Other':<14}  {(~df.known).sum():>7,}"))
leg = ax.legend(handles=handles, loc="lower left", bbox_to_anchor=(0.01, 0.01),
                ncol=2, frameon=False, fontsize=11, labelcolor="#e0dccf",
                handletextpad=0.4, columnspacing=1.8, labelspacing=0.45,
                prop={"family": "monospace", "size": 11})

out = OUT / "dotmap_dark.png"
fig.savefig(out, dpi=180, facecolor=fig.get_facecolor(), bbox_inches="tight", pad_inches=0.3)
plt.close(fig)
print(f"-> {out} ({out.stat().st_size/1e6:.1f} MB)")


# ============================================================
# 2) NORWAY MAPLE vs EVERYTHING ELSE
# ============================================================
df["is_norway"] = df["botanical_key"] == "acer platanoides"
print(f"Norway maple: {df.is_norway.sum():,}")

fig, ax = figure_for_map(w=24)
rest = df[~df.is_norway]
nm = df[df.is_norway]
ax.scatter(rest.lon, rest.lat, c="#3a4049", s=1.0, alpha=0.5, linewidths=0)
ax.scatter(nm.lon, nm.lat, c="#e23c3c", s=1.6, alpha=0.85, linewidths=0)

add_titles(ax, "The Norway maple, mapped",
           "69,563 Norway maples (red) — Toronto's #1 street tree, an invasive Europeans planted here by the tens of thousands")

handles = [
    Line2D([0], [0], marker="o", color="none", markerfacecolor="#e23c3c",
           markersize=11, label="Norway maple (Acer platanoides) — 69,563"),
    Line2D([0], [0], marker="o", color="none", markerfacecolor="#6d7380",
           markersize=11, label="All other species — 619,450"),
]
ax.legend(handles=handles, loc="lower left", bbox_to_anchor=(0.01, 0.01),
          frameon=False, fontsize=12, labelcolor="#e0dccf")

out = OUT / "dotmap_norway.png"
fig.savefig(out, dpi=180, facecolor=fig.get_facecolor(), bbox_inches="tight", pad_inches=0.3)
plt.close(fig)
print(f"-> {out} ({out.stat().st_size/1e6:.1f} MB)")


# ============================================================
# 3) NATIVE vs INTRODUCED
# ============================================================
species_to = json.loads((ROOT / "site" / "data" / "species_toronto.json").read_text())


def native_bucket(key):
    v = species_to.get(key)
    if not v:
        return "unknown"
    n = (v.get("native") or "").lower()
    if "native to ontario" in n or "hybrid of ontario" in n:
        return "native_on"
    if "north america" in n:
        return "native_na"
    if "introduced" in n or "hybrid (north" in n:
        return "introduced"
    return "unknown"


# Match by (genus species)
df["bucket"] = df["botanical_key"].apply(lambda k: native_bucket(" ".join(k.split()[:2]) if k else None))
print(df.bucket.value_counts())

bucket_colors = {
    "native_on":  "#4fb58a",
    "native_na":  "#94c961",
    "introduced": "#d65a5a",
    "unknown":    "#4a4d55",
}

fig, ax = figure_for_map(w=24)
for b in ["unknown", "introduced", "native_na", "native_on"]:  # unknown first (behind)
    sub = df[df.bucket == b]
    if len(sub) == 0:
        continue
    alpha = 0.35 if b == "unknown" else 0.8
    ax.scatter(sub.lon, sub.lat, c=bucket_colors[b], s=1.3, alpha=alpha, linewidths=0)

add_titles(ax, "Native vs. introduced, by tree",
           "Based on Toronto's species-planted-on-streets list — unknown = species not on the city's current-planting guide")

counts = df.bucket.value_counts()
handles = [
    Line2D([0], [0], marker="o", color="none", markerfacecolor=bucket_colors["native_on"],
           markersize=11, label=f"Native to Ontario  ({counts.get('native_on', 0):,})"),
    Line2D([0], [0], marker="o", color="none", markerfacecolor=bucket_colors["native_na"],
           markersize=11, label=f"Native to North America  ({counts.get('native_na', 0):,})"),
    Line2D([0], [0], marker="o", color="none", markerfacecolor=bucket_colors["introduced"],
           markersize=11, label=f"Introduced  ({counts.get('introduced', 0):,})"),
    Line2D([0], [0], marker="o", color="none", markerfacecolor=bucket_colors["unknown"],
           markersize=11, label=f"Not on current planting list  ({counts.get('unknown', 0):,})"),
]
ax.legend(handles=handles, loc="lower left", bbox_to_anchor=(0.01, 0.01),
          frameon=False, fontsize=12, labelcolor="#e0dccf")

out = OUT / "dotmap_native.png"
fig.savefig(out, dpi=180, facecolor=fig.get_facecolor(), bbox_inches="tight", pad_inches=0.3)
plt.close(fig)
print(f"-> {out} ({out.stat().st_size/1e6:.1f} MB)")
