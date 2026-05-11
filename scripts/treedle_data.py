"""Pre-compute the daily-tree puzzle set for Treedle.

For each of N days (365), pick one specific Toronto tree as that day's puzzle.
Criteria for inclusion:
  - Species is in the calendar.json curated roster (60 species — players need a
    finite, learnable answer set)
  - Tree has a substantial trunk (DBH 25-180 cm) — Street View shows it clearly
  - Lat/lon is valid and inside Toronto
  - Address is parseable

Output: site/data/treedle.json with the full puzzle set + an answer pool.
"""
from __future__ import annotations
from pathlib import Path
import json
import random

import duckdb

ROOT = Path(__file__).resolve().parent.parent
PROC = ROOT / "data" / "processed"
OUT = ROOT / "site" / "data" / "treedle.json"

NUM_PUZZLES = 365
SEED = 42  # deterministic puzzle set


def pretty_common(name: str | None) -> str:
    if not name:
        return "—"
    name = name.strip()
    if "," in name:
        family, cultivar = [p.strip() for p in name.split(",", 1)]
        return f"{cultivar} {family}".title()
    return name.title()


def main():
    con = duckdb.connect()
    calendar = json.loads((ROOT / "site" / "data" / "calendar.json").read_text())
    # Build the answer pool: the curated 60 species, with their common + genus
    answer_pool = []
    for sp in calendar:
        botanical = sp.get("botanical", sp["key"])
        common = pretty_common(sp.get("common", sp["key"]))
        genus = sp["key"].split()[0] if " " in sp["key"] else sp["key"]
        answer_pool.append({
            "key": sp["key"],
            "common": common,
            "botanical": botanical,
            "genus": genus,
        })
    answer_keys = {a["key"] for a in answer_pool}

    # Candidate trees: substantial DBH, in roster, with neighbourhood
    candidates = con.execute(f"""
        SELECT _id, ADDRESS, STREETNAME, SUFFIX, common_raw, botanical_key,
               botanical_raw, dbh_cm, lat, lon, nbhd_name
        FROM read_parquet('{PROC}/trees.parquet')
        WHERE botanical_key IN ({','.join("'" + k.replace("'", "''") + "'" for k in answer_keys)})
          AND dbh_cm BETWEEN 25 AND 180
          AND lat BETWEEN 43.58 AND 43.86
          AND lon BETWEEN -79.65 AND -79.11
          AND nbhd_name IS NOT NULL
          AND ADDRESS IS NOT NULL AND STREETNAME IS NOT NULL
    """).fetchdf()
    print(f"candidate pool: {len(candidates):,} trees")

    # Sample with stratified species coverage — aim for variety across answers
    rng = random.Random(SEED)
    by_species = candidates.groupby("botanical_key")
    selected = []

    # Per-species cap so dominant species (Norway maple, honey locust) don't
    # monopolize the puzzle calendar. 12 per species max → 60 species × 12 = 720
    # potential picks, well over NUM_PUZZLES.
    per_species_cap = 12
    pool_per_species = {}
    for key, grp in by_species:
        rows = grp.to_dict(orient="records")
        rng.shuffle(rows)
        pool_per_species[key] = rows[:per_species_cap]

    # Round-robin across species so successive days don't repeat species often
    species_order = sorted(pool_per_species.keys())
    rng.shuffle(species_order)
    cursors = {k: 0 for k in species_order}

    while len(selected) < NUM_PUZZLES:
        progress = False
        for key in species_order:
            if cursors[key] < len(pool_per_species[key]):
                selected.append(pool_per_species[key][cursors[key]])
                cursors[key] += 1
                progress = True
                if len(selected) >= NUM_PUZZLES:
                    break
        if not progress:
            break

    print(f"selected {len(selected)} puzzles across {len(set(s['botanical_key'] for s in selected))} species")

    # Shrink each row to just what the client needs
    def clean_part(p):
        s = str(p or "").strip()
        return "" if s.lower() in {"", "none", "nan"} else s

    puzzles = []
    for r in selected:
        addr_parts = [clean_part(r["ADDRESS"]), clean_part(r["STREETNAME"]), clean_part(r["SUFFIX"])]
        addr = " ".join(p for p in addr_parts if p)
        puzzles.append({
            "k": r["botanical_key"],                          # answer key
            "lat": round(float(r["lat"]), 5),
            "lon": round(float(r["lon"]), 5),
            "dbh": int(r["dbh_cm"]),
            "nbhd": r["nbhd_name"],
            "addr": addr,
        })

    OUT.write_text(json.dumps({
        "version": 1,
        "puzzles": puzzles,
        "answers": answer_pool,
    }, separators=(",", ":")))
    print(f"wrote {OUT.relative_to(ROOT)} — {len(json.dumps(puzzles)):,} bytes puzzles, "
          f"{len(answer_pool)} answers")


if __name__ == "__main__":
    main()
