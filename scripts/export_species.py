"""Export per-species JSON + a species index for the search box.

Layout:
  site/data/species_index.json    — list of {key, common, botanical, n, slug}
  site/data/species/<slug>.json   — all specimens of that species
                                    {"key":..., "common":..., "botanical":...,
                                     "n":..., "trees": [[lat,lon,dbh], ...],
                                     "top_nbhds": [[name, n], ...]}

Species key = genus + species normalized (matches the keys used by
species.json + species_toronto.json).
"""
from pathlib import Path
import json
import re
import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
TREES = ROOT / "data" / "processed" / "trees.parquet"
SITE_DATA = ROOT / "site" / "data"
SPECIES_DIR = SITE_DATA / "species"
SPECIES_DIR.mkdir(parents=True, exist_ok=True)


def species_key(botanical_raw: str) -> str | None:
    if not botanical_raw:
        return None
    s = botanical_raw.lower().strip()
    s = re.sub(r"'[^']*'", "", s)
    s = re.sub(r"\b(f|var|subsp|ssp|cv|x)\.?\b", " ", s)
    s = re.sub(r"[^a-z\- ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    parts = s.split(" ")
    if len(parts) < 2:
        return parts[0] if parts else None
    return f"{parts[0]} {parts[1]}"


def slug(name: str) -> str:
    s = re.sub(r"[^A-Z0-9]+", "_", name.upper()).strip("_")
    return s or "UNKNOWN"


con = duckdb.connect()
df = con.execute(f"""
    SELECT botanical_raw, common_raw, lat, lon, dbh_cm, nbhd_name
    FROM read_parquet('{TREES}')
    WHERE botanical_raw IS NOT NULL AND lat IS NOT NULL AND lon IS NOT NULL
""").fetchdf()
df["key"] = df["botanical_raw"].map(species_key)
df = df[df["key"].notna()]
print(f"loaded {len(df):,} trees across {df['key'].nunique()} species keys")

# Pick the most-common common_raw per key as the display name
common_pref = (df.groupby(["key", "common_raw"]).size()
               .reset_index(name="n")
               .sort_values(["key", "n"], ascending=[True, False])
               .drop_duplicates("key"))
bot_pref = (df.groupby(["key", "botanical_raw"]).size()
            .reset_index(name="n")
            .sort_values(["key", "n"], ascending=[True, False])
            .drop_duplicates("key"))

index_rows = []
for key, g in df.groupby("key", sort=False):
    common = common_pref.loc[common_pref.key == key, "common_raw"].iloc[0]
    bot = bot_pref.loc[bot_pref.key == key, "botanical_raw"].iloc[0]
    # Strip cultivar off botanical for display
    bot_display = re.sub(r"'[^']*'", "", bot).strip()
    sl = slug(key)
    trees = [
        [round(float(r.lat), 5),
         round(float(r.lon), 5),
         None if pd.isna(r.dbh_cm) else int(r.dbh_cm)]
        for r in g.itertuples(index=False)
    ]
    top_nbhds = (g.groupby("nbhd_name").size()
                 .sort_values(ascending=False)
                 .head(10)
                 .reset_index()
                 .values.tolist())
    top_nbhds = [[str(n), int(c)] for n, c in top_nbhds if n]
    payload = {
        "key": key,
        "common": common,
        "botanical": bot_display,
        "n": int(len(g)),
        "trees": trees,
        "top_nbhds": top_nbhds,
    }
    out = SPECIES_DIR / f"{sl}.json"
    out.write_text(json.dumps(payload, separators=(",", ":")))
    index_rows.append({
        "key": key,
        "common": common,
        "botanical": bot_display,
        "n": int(len(g)),
        "slug": sl,
    })

# Sort index: most common first
index_rows.sort(key=lambda r: -r["n"])
(SITE_DATA / "species_index.json").write_text(json.dumps(index_rows, separators=(",", ":")))
total_bytes = sum((SPECIES_DIR / f"{r['slug']}.json").stat().st_size for r in index_rows)
print(f"wrote {len(index_rows)} species files, {total_bytes/1e6:.1f} MB total")
print(f"species_index.json: {(SITE_DATA / 'species_index.json').stat().st_size/1024:.1f} KB")
