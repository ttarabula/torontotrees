"""Build site/data/calendar.json with bloom + fall-colour timing per species.

Timing is rough averages for Toronto (zone 6b/7a). Off by a week or two
year-to-year is expected. Sources: Toronto's species-planted-on-streets
page, Wikipedia, standard dendrology references (Dirr, Farrar).

Output shape: list of
  {key, common, botanical, n, bloom:{start, end, note, kind}, fall:{...}, tags}

Tags:
  "showy_bloom"    — notable flowers people go out to see
  "fragrant_bloom" — scent is the draw even if flowers are small
  "showy_fall"     — big autumn colour draw
  "coast_winter"   — interesting bark or form in winter
"""
from pathlib import Path
import json

ROOT = Path(__file__).resolve().parent.parent
IDX = ROOT / "site" / "data" / "species_index.json"
OUT = ROOT / "site" / "data" / "calendar.json"


def w(m, d): return {"m": m, "d": d}


# Species entries. Not all species have a bloom OR fall entry; conifers
# typically have neither. Dates are Toronto-zone averages.
SPECIES = {
    # --- SPRING SHOWY BLOOMERS -----------------------------------------
    "prunus serrulata": {
        "bloom": (4, 28, 5, 10, "pink-white cherry blossom, iconic", "showy_bloom"),
        "fall":  (10, 5, 10, 20, "bronze-red"),
    },
    "prunus sargentii": {
        "bloom": (4, 18, 4, 30, "pale pink, very early, flat-topped crown", "showy_bloom"),
        "fall":  (9, 25, 10, 15, "scarlet"),
    },
    "prunus yedoensis": {
        "bloom": (4, 25, 5, 5, "white-pink, the Tidal Basin cherry", "showy_bloom"),
    },
    "prunus subhirtella": {
        "bloom": (4, 15, 4, 30, "pale pink, weeping form", "showy_bloom"),
    },
    "prunus persica": {
        "bloom": (4, 10, 4, 22, "deep pink peach blossom", "showy_bloom"),
    },
    "prunus armeniaca": {
        "bloom": (4, 5, 4, 20, "white-pink apricot blossom, very early", "showy_bloom"),
    },
    "prunus virginiana": {
        "bloom": (5, 10, 5, 25, "white racemes, mildly fragrant", "showy_bloom"),
    },
    "prunus spp": {
        "bloom": (4, 20, 5, 15, "white-to-pink cherry/plum blossoms"),
        "fall":  (9, 30, 10, 20, "red-orange"),
    },
    "malus sargentii": {
        "bloom": (5, 1, 5, 12, "white, profuse, small red fruit persists into winter", "showy_bloom"),
    },
    "amelanchier canadensis": {
        "bloom": (4, 22, 5, 5, "white, among the earliest deciduous bloomers", "showy_bloom"),
        "fall":  (9, 25, 10, 15, "orange-red", "showy_fall"),
    },
    "cercis canadensis": {
        "bloom": (4, 28, 5, 12, "magenta-pink, clinging to bare branches", "showy_bloom"),
        "fall":  (10, 5, 10, 25, "yellow"),
    },
    "magnolia soulngeana": {
        "bloom": (4, 20, 5, 5, "large pink-white saucers, frost-risk", "showy_bloom"),
    },
    "pyrus calleryana": {
        "bloom": (4, 22, 5, 5, "white, profuse, briefly unpleasant smell", "showy_bloom"),
        "fall":  (10, 20, 11, 10, "burgundy-purple", "showy_fall"),
    },

    # --- LATE SPRING / EARLY SUMMER SHOWY ------------------------------
    "aesculus hippocastanum": {
        "bloom": (5, 12, 5, 25, "large white panicles, red-throat", "showy_bloom"),
        "fall":  (9, 25, 10, 20, "yellow-brown, often scorched"),
    },
    "aesculus glabra": {
        "bloom": (5, 10, 5, 22, "cream-yellow panicles", "showy_bloom"),
        "fall":  (9, 20, 10, 10, "orange", "showy_fall"),
    },
    "aesculus flava": {
        "bloom": (5, 15, 5, 28, "yellow panicles", "showy_bloom"),
        "fall":  (9, 25, 10, 15, "orange-yellow"),
    },
    "aesculus carnea": {
        "bloom": (5, 15, 5, 30, "red-pink panicles, 'Briotii'", "showy_bloom"),
    },
    "catalpa speciosa": {
        "bloom": (6, 5, 6, 20, "large white panicles, purple-throat", "showy_bloom"),
        "fall":  (10, 1, 10, 20, "yellow"),
    },
    "liriodendron tulipifera": {
        "bloom": (5, 25, 6, 15, "green-orange tulip-shaped, high in the canopy", "showy_bloom"),
        "fall":  (9, 25, 10, 20, "golden yellow", "showy_fall"),
    },
    "syringa reticulata": {
        "bloom": (6, 5, 6, 20, "creamy white panicles, privet-honey fragrance", "showy_bloom"),
    },

    # --- FRAGRANT BLOOMERS ---------------------------------------------
    "robinia pseudoacacia": {
        "bloom": (5, 28, 6, 10, "white racemes, intensely honey-scented", "fragrant_bloom"),
        "fall":  (10, 1, 10, 20, "yellow"),
    },
    "tilia cordata": {
        "bloom": (6, 18, 7, 2, "small fragrant yellow, bees lose their minds", "fragrant_bloom"),
        "fall":  (10, 5, 10, 25, "pale yellow"),
    },
    "tilia americana": {
        "bloom": (6, 15, 6, 30, "fragrant pale-yellow, like cordata but bigger leaves", "fragrant_bloom"),
        "fall":  (10, 1, 10, 20, "pale yellow"),
    },
    "tilia": {
        "bloom": (6, 15, 6, 30, "fragrant pale-yellow", "fragrant_bloom"),
        "fall":  (10, 1, 10, 20, "pale yellow"),
    },
    "cercidiphyllum japonicum": {
        "bloom": (4, 10, 4, 22, "inconspicuous tiny red", ""),
        "fall":  (10, 5, 10, 20, "orange-peach-pink, leaves smell like caramel", "showy_fall"),
    },
    "cladrastis kentukea": {
        "bloom": (6, 1, 6, 15, "white pea-family racemes, fragrant, intermittent years", "fragrant_bloom"),
        "fall":  (10, 5, 10, 25, "golden yellow"),
    },

    # --- FALL-COLOUR STARS ---------------------------------------------
    "acer saccharum": {
        "fall":  (9, 25, 10, 20, "orange to scarlet, the iconic Canadian autumn tree", "showy_fall"),
    },
    "acer rubrum": {
        "fall":  (9, 20, 10, 15, "deep red, often earliest of the maples", "showy_fall"),
    },
    "acer freemanii": {
        "fall":  (9, 25, 10, 15, "brilliant orange-red ('Autumn Blaze' cultivar)", "showy_fall"),
    },
    "acer nigrum": {
        "fall":  (9, 25, 10, 20, "yellow to orange, sugar-maple cousin", "showy_fall"),
    },
    "acer platanoides": {
        "fall":  (10, 15, 11, 5, "yellow, reliable if unspectacular, drops late", ""),
    },
    "acer saccharinum": {
        "fall":  (9, 30, 10, 20, "pale yellow, fast turn"),
    },
    "acer palmatum": {
        "fall":  (10, 10, 11, 1, "red-purple, late and long-held", "showy_fall"),
    },
    "acer tataricum": {
        "fall":  (9, 20, 10, 10, "red-yellow-orange at once"),
    },
    "quercus rubra": {
        "fall":  (10, 15, 11, 5, "deep red, holds leaves late", "showy_fall"),
    },
    "quercus alba": {
        "fall":  (10, 15, 11, 10, "wine-red to russet, often marcescent", "showy_fall"),
    },
    "quercus macrocarpa": {
        "fall":  (10, 10, 10, 30, "gold-brown, long-lived tree"),
    },
    "quercus bicolor": {
        "fall":  (10, 15, 11, 5, "yellow-brown, swamp-edge species"),
    },
    "quercus palustris": {
        "fall":  (10, 20, 11, 10, "deep red, holds leaves through winter (marcescent)", "showy_fall"),
    },
    "quercus robur": {
        "fall":  (10, 25, 11, 15, "russet-brown, late, European species"),
    },
    "ginkgo biloba": {
        "fall":  (10, 25, 11, 10, "bright gold, famous overnight synchronous drop", "showy_fall"),
    },
    "betula papyrifera": {
        "fall":  (9, 25, 10, 15, "clear yellow against white bark", "showy_fall"),
    },
    "betula": {
        "fall":  (9, 25, 10, 15, "yellow", "showy_fall"),
    },
    "fagus sylvatica": {
        "fall":  (10, 20, 11, 15, "copper-bronze, leaves held into winter (marcescent)", "showy_fall"),
    },
    "ostrya virginiana": {
        "fall":  (10, 5, 10, 25, "yellow, hop-like seed clusters", "showy_fall"),
    },
    "taxodium distichum": {
        "fall":  (10, 25, 11, 15, "coppery orange, deciduous conifer", "showy_fall"),
    },

    # --- MODEST BLOOM + FALL --------------------------------------------
    "gleditsia triacanthos": {
        "bloom": (6, 5, 6, 15, "inconspicuous green, easily missed", ""),
        "fall":  (10, 15, 11, 5, "clear yellow, tiny leaves drop cleanly"),
    },
    "gymnocladus dioicus": {
        "bloom": (5, 25, 6, 10, "greenish-white, inconspicuous", ""),
        "fall":  (10, 10, 10, 30, "yellow, bipinnate leaves"),
    },
    "celtis occidentalis": {
        "fall":  (10, 1, 10, 20, "yellow"),
    },
    "platanus acerifolia": {
        "fall":  (10, 15, 11, 5, "yellow-brown, slow shed"),
    },
    "ulmus pumila": {
        "fall":  (10, 10, 10, 25, "yellow"),
    },
    "ulmus americana": {
        "fall":  (10, 1, 10, 20, "golden yellow, Dutch-elm survivors"),
    },
    "ulmus davidiana": {
        "fall":  (10, 5, 10, 25, "yellow ('Accolade' resistant cultivar)"),
    },
    "ulmus": {
        "fall":  (10, 5, 10, 25, "yellow"),
    },
    "fraxinus pennsylvanica": {
        "fall":  (9, 25, 10, 10, "yellow, mostly EAB-killed"),
    },
    "fraxinus americana": {
        "fall":  (9, 25, 10, 10, "purple to yellow, rare post-EAB"),
    },
    "morus alba": {
        "fall":  (9, 25, 10, 15, "yellow, edible berries earlier"),
    },
    "acer negundo": {
        "fall":  (9, 15, 10, 5, "yellow, early shed"),
    },
    "acer campestre": {
        "fall":  (10, 5, 10, 25, "yellow-orange, small European maple"),
    },
    "acer": {
        "fall":  (10, 1, 10, 20, "yellow to red", "showy_fall"),
    },

    # --- CONIFERS (evergreen, no fall/bloom events) --------------------
    "picea pungens": {},
    "picea glauca": {},
    "picea abies": {},
    "picea": {},
    "pinus nigra": {},
    "pinus strobus": {},
    "pinus sylvestris": {},
    "thuja occidentalis": {},
}


def tuplify(x):
    """(4, 28, 5, 10, 'note', 'tag') → dict structure + tags."""
    if not x: return None, []
    if len(x) == 5:
        ms, ds, me, de, note = x
        return {"start": w(ms, ds), "end": w(me, de), "note": note}, []
    ms, ds, me, de, note, tag = x
    return {"start": w(ms, ds), "end": w(me, de), "note": note}, ([tag] if tag else [])


def main():
    idx = json.loads(IDX.read_text())
    idx_by_key = {s["key"]: s for s in idx}

    records = []
    missing = []
    for key, data in SPECIES.items():
        meta = idx_by_key.get(key)
        if not meta:
            missing.append(key)
            continue
        bloom, btags = tuplify(data.get("bloom"))
        fall, ftags = tuplify(data.get("fall"))
        tags = sorted(set(btags + ftags))
        # Skip entries with literally nothing (conifers)
        if not bloom and not fall:
            continue
        records.append({
            "key": key,
            "common": meta["common"],
            "botanical": meta["botanical"],
            "slug": meta["slug"],
            "n": meta["n"],
            "bloom": bloom,
            "fall": fall,
            "tags": tags,
        })

    records.sort(key=lambda r: -r["n"])
    OUT.write_text(json.dumps(records, separators=(",", ":")))
    print(f"wrote {OUT} — {len(records)} species with calendar data")
    if missing:
        print(f"  (skipped {len(missing)} unmatched keys: {missing[:6]}…)")

    # Sanity check: distribution of tags
    from collections import Counter
    tag_counts = Counter(t for r in records for t in r["tags"])
    print("tag counts:", dict(tag_counts))


if __name__ == "__main__":
    main()
