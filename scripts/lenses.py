"""Generate "data lens" pages — curated filtered views of the street-tree
inventory.

Each lens is:
  - a filter predicate over trees.parquet
  - a hero PNG dot-map (matches in lens-colour, rest dim grey)
  - a JSON of sample-notable-trees with permalinks
  - a per-lens HTML page

Output:
  site/charts/lens/<slug>.png
  site/data/lens/<slug>.json
  site/lenses/<slug>/index.html
  site/lenses/index.html  (gallery)
"""
from __future__ import annotations
from pathlib import Path
import html
import json

import duckdb
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent.parent
PROC = ROOT / "data" / "processed"
CHARTS = ROOT / "site" / "charts" / "lens"
DATA = ROOT / "site" / "data" / "lens"
PAGES = ROOT / "site" / "lenses"
CHARTS.mkdir(parents=True, exist_ok=True)
DATA.mkdir(parents=True, exist_ok=True)
PAGES.mkdir(parents=True, exist_ok=True)

LAT_MIN, LAT_MAX = 43.58, 43.86
LON_MIN, LON_MAX = -79.65, -79.11
BG_DARK = "#0b0d10"
DIM = "#2a3a36"

# Lens definitions: each gets a slug, title, subtitle, SQL predicate, colour,
# and "notable" picker (which trees to feature on the page).
LENSES = [
    {
        "slug": "veterans",
        "title": "Veterans — Toronto's catalogued elders",
        "kicker": "Trees with trunk diameter ≥ 100 cm",
        "intro": "Toronto's street-tree inventory caps DBH at 250 cm, but the genuinely-large trees — those with trunk diameters of one metre or more — are a small, scattered population of 6,400-odd specimens, almost all of them more than a century old. This is what's left of the pre-war canopy plus the deliberately-protected veterans of the contemporary city.",
        "predicate": "dbh_cm BETWEEN 100 AND 200",
        "colour": "#e3b14d",
        "notable_order": "dbh_cm DESC",
        "notable_limit": 12,
    },
    {
        "slug": "pollinator-hosts",
        "title": "Pollinator hosts — the trees the food web actually depends on",
        "kicker": "Native Quercus, Prunus, Salix, Betula, Malus, and native Acer species",
        "intro": "Doug Tallamy's host-plant research splits Toronto's street trees into two unequal halves: the species that anchor native insect populations (and through them, the city's bird populations) and everything else. This map shows the first half — every native-genus oak, cherry, willow, birch, crabapple, and native maple in the inventory.",
        "predicate": (
            "split_part(botanical_key, ' ', 1) IN ('quercus','prunus','betula','salix','malus') "
            "OR botanical_key LIKE 'acer saccharum%' "
            "OR botanical_key LIKE 'acer rubrum%' "
            "OR botanical_key LIKE 'acer saccharinum%' "
            "OR botanical_key LIKE 'acer nigrum%' "
            "OR botanical_key LIKE 'acer negundo%' "
            "OR botanical_key LIKE 'acer freemanii%'"
        ),
        "colour": "#6bb76b",
        "notable_order": "dbh_cm DESC",
        "notable_limit": 12,
    },
    {
        "slug": "spring-bloomers",
        "title": "Spring bloomers — the showy April-to-June canopy",
        "kicker": "Magnolia, cherry, plum, crabapple, redbud, serviceberry, horsechestnut, lilac, catalpa, tulip tree",
        "intro": "Most Toronto street trees flower so quietly nobody notices. This lens shows the ones that don't: the genera flagged for showy spring bloom on the City's planting list. In sequence, between roughly April 5 and June 15, these species turn whole blocks pink, white, magenta, or chartreuse for two weeks at a time.",
        "predicate": (
            "split_part(botanical_key, ' ', 1) IN "
            "('magnolia','prunus','malus','cercis','amelanchier','aesculus','catalpa','liriodendron','syringa')"
        ),
        "colour": "#c6256b",
        "notable_order": "dbh_cm DESC",
        "notable_limit": 12,
    },
    {
        "slug": "rarities",
        "title": "Rarities — species with fewer than 50 trees citywide",
        "kicker": "81 different species, 1,207 trees total",
        "intro": "Toronto's planting list has a long tail. After the 100,000 Norway maples and 60,000 honey locusts and the rest of the dominant cohort, there are 81 species each represented by fewer than 50 trees in the entire city. Most are recent ornamental experiments; a few are Carolinian natives at the northern edge of their range. All of them are worth a detour if you're nearby.",
        "predicate": """botanical_key IN (
            SELECT botanical_key FROM read_parquet('{PROC}/trees.parquet')
            WHERE botanical_key IS NOT NULL
            GROUP BY botanical_key HAVING COUNT(*) < 50
        )""",
        "colour": "#a260c4",
        "notable_order": "dbh_cm DESC",
        "notable_limit": 14,
        "group_by_species": True,
    },
]


def render_lens_png(con, lens):
    """Hero PNG: matches in lens colour over a dim full-city dot map."""
    # Subsample full city for context layer (50K dim points)
    ctx = con.execute(f"""
        SELECT lat, lon FROM read_parquet('{PROC}/trees.parquet')
        WHERE lat BETWEEN {LAT_MIN} AND {LAT_MAX}
          AND lon BETWEEN {LON_MIN} AND {LON_MAX}
        USING SAMPLE 50000
    """).fetchdf()

    pred = lens["predicate"].format(PROC=PROC)
    matches = con.execute(f"""
        SELECT lat, lon FROM read_parquet('{PROC}/trees.parquet')
        WHERE lat BETWEEN {LAT_MIN} AND {LAT_MAX}
          AND lon BETWEEN {LON_MIN} AND {LON_MAX}
          AND ({pred})
    """).fetchdf()
    n_matches = len(matches)

    fig, ax = plt.subplots(figsize=(11, 6.5), dpi=110)
    fig.patch.set_facecolor(BG_DARK)
    ax.set_facecolor(BG_DARK)
    ax.scatter(ctx["lon"], ctx["lat"], s=0.18, c=DIM, alpha=0.6, linewidths=0)
    ax.scatter(matches["lon"], matches["lat"], s=0.7, c=lens["colour"], alpha=0.85, linewidths=0)
    ax.set_xlim(LON_MIN, LON_MAX)
    ax.set_ylim(LAT_MIN, LAT_MAX)
    ax.set_aspect(1 / 0.72)
    ax.axis("off")
    fig.tight_layout(pad=0)
    out = CHARTS / f"{lens['slug']}.webp"
    fig.savefig(out, facecolor=BG_DARK, bbox_inches="tight", pad_inches=0.05,
                pil_kwargs={"quality": 80, "method": 6})
    plt.close(fig)
    return n_matches, out


def render_lens_data(con, lens):
    """Notable-tree list (sample) + a per-species count for the page."""
    pred = lens["predicate"].format(PROC=PROC)

    if lens.get("group_by_species"):
        # For Rarities: pick one notable specimen per rare species (the largest)
        notable_q = f"""
        WITH ranked AS (
            SELECT _id, ADDRESS, STREETNAME, SUFFIX, common_raw, botanical_raw,
                   botanical_key, dbh_cm, lat, lon, nbhd_name,
                   ROW_NUMBER() OVER (PARTITION BY botanical_key ORDER BY dbh_cm DESC NULLS LAST) AS rk
            FROM read_parquet('{PROC}/trees.parquet')
            WHERE ({pred}) AND dbh_cm IS NOT NULL AND dbh_cm < 200
        )
        SELECT * FROM ranked WHERE rk = 1 ORDER BY dbh_cm DESC LIMIT {lens['notable_limit']}
        """
    else:
        notable_q = f"""
        SELECT _id, ADDRESS, STREETNAME, SUFFIX, common_raw, botanical_raw,
               botanical_key, dbh_cm, lat, lon, nbhd_name
        FROM read_parquet('{PROC}/trees.parquet')
        WHERE ({pred}) AND dbh_cm < 200
        ORDER BY {lens['notable_order']}
        LIMIT {lens['notable_limit']}
        """
    notable = con.execute(notable_q).fetchdf().to_dict(orient="records")

    # Per-species counts within the lens (for narrative)
    species_q = f"""
    SELECT common_raw, botanical_raw, botanical_key, COUNT(*) n
    FROM read_parquet('{PROC}/trees.parquet')
    WHERE ({pred})
    GROUP BY 1, 2, 3 ORDER BY 4 DESC LIMIT 10
    """
    species = con.execute(species_q).fetchdf().to_dict(orient="records")

    out = DATA / f"{lens['slug']}.json"
    out.write_text(json.dumps({"notable": notable, "species": species}, default=str, indent=2))
    return notable, species


def pretty_common(name: str | None) -> str:
    if not name:
        return "—"
    name = name.strip()
    if "," in name:
        family, cultivar = [p.strip() for p in name.split(",", 1)]
        return f"{cultivar} {family}".title()
    return name.title()


def clean_part(p) -> str:
    s = str(p or "").strip()
    return "" if s.lower() in {"", "none", "nan"} else s


def render_lens_page(lens, n_matches, notable, species):
    title = lens["title"]
    desc = lens["intro"][:160].rstrip()
    sample_html_parts = []
    for t in notable:
        addr = " ".join(p for p in [
            clean_part(t.get("ADDRESS")), clean_part(t.get("STREETNAME")), clean_part(t.get("SUFFIX"))
        ] if p)
        common = pretty_common(t.get("common_raw"))
        bot = t.get("botanical_raw") or ""
        dbh = int(t.get("dbh_cm") or 0)
        nbhd = t.get("nbhd_name") or ""
        permalink = f"../../#@{float(t['lat']):.5f},{float(t['lon']):.5f}"
        sample_html_parts.append(
            f"""<li>
              <strong>{html.escape(common)}</strong> <span class="bot">{html.escape(bot)}</span><br>
              <span class="meta">{dbh} cm DBH · {html.escape(addr)} · <a href="{permalink}">view →</a>{f' · <span class="nbhd">{html.escape(nbhd)}</span>' if nbhd else ''}</span>
            </li>"""
        )
    sample_html = "<ul class='samples'>" + "".join(sample_html_parts) + "</ul>"

    species_rows = []
    for s in species[:6]:
        common = pretty_common(s.get("common_raw"))
        bot = s.get("botanical_raw") or ""
        species_rows.append(
            f"<tr><td>{html.escape(common)} <em class='bot'>{html.escape(bot)}</em></td>"
            f"<td class='num'>{int(s['n']):,}</td></tr>"
        )
    species_table = (
        "<table class='species'><thead><tr><th>Top species in this lens</th><th class='num'>Trees</th></tr></thead>"
        "<tbody>" + "".join(species_rows) + "</tbody></table>"
    )

    page = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(title)} — torontotrees lens</title>
<meta name="description" content="{html.escape(desc)}">
<link rel="icon" href="data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'><text y='52' font-size='54'>🔍</text></svg>">
<meta property="og:title" content="{html.escape(title)}">
<meta property="og:description" content="{html.escape(desc)}">
<meta property="og:type" content="article">
<meta property="og:image" content="https://treeto.ca/charts/lens/{lens['slug']}.webp">
<meta name="twitter:card" content="summary_large_image">
<style>
  :root {{ --fg:#1a1a1a; --muted:#666; --accent:#2b7a3d; --accent-dark:#1e5b2d; --bg:#faf7f2; --card:#fff; --border:#e2e2e2; --rule:#d9d4c7; --lens:{lens['colour']}; }}
  * {{ box-sizing: border-box; }}
  body {{ font: 17px/1.65 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; background: var(--bg); color: var(--fg); -webkit-font-smoothing: antialiased; }}
  header.site {{ border-bottom: 1px solid var(--rule); padding: 1rem 1.25rem; }}
  header.site .inner {{ max-width: 920px; margin: 0 auto; display: flex; justify-content: space-between; align-items: center; gap: 1rem 1.5rem; flex-wrap: wrap; }}
  header.site a.home {{ color: var(--accent-dark); text-decoration: none; font-weight: 600; }}
  header.site a.home::before {{ content: "← "; opacity: 0.55; }}
  header.site nav.mainnav {{ display: flex; gap: 1.1rem; font-size: .9rem; flex-wrap: wrap; }}
  header.site nav.mainnav a {{ color: #666; text-decoration: none; }}
  header.site nav.mainnav a:hover {{ color: var(--accent-dark); text-decoration: underline; }}
  main {{ max-width: 920px; margin: 0 auto; padding: 2rem 1.25rem 3rem; }}
  .kicker {{ color: var(--lens); font-size: .92rem; font-weight: 600; margin: 0 0 .35rem; text-transform: uppercase; letter-spacing: .03em; }}
  h1 {{ font-size: clamp(1.7rem, 4vw, 2.3rem); line-height: 1.18; margin: .25rem 0 .5rem; }}
  .count {{ color: var(--muted); font-size: 1rem; margin-bottom: 1.5rem; }}
  .count strong {{ color: var(--lens); font-size: 1.2rem; }}
  .hero {{ margin: 1.5rem -1.25rem; }}
  .hero img {{ width: 100%; height: auto; display: block; background: #0b0d10; }}
  .intro {{ font-size: 1.05rem; line-height: 1.65; max-width: 720px; margin: 1.5rem 0; }}
  h2 {{ font-size: 1.25rem; margin: 2rem 0 .75rem; padding-top: 1.5rem; border-top: 1px solid var(--rule); }}
  table.species {{ width: 100%; max-width: 560px; border-collapse: collapse; margin: 1rem 0 1.5rem; font-size: .95rem; background: var(--card); border: 1px solid var(--border); border-radius: 6px; overflow: hidden; }}
  table.species th, table.species td {{ padding: .55rem .75rem; text-align: left; border-bottom: 1px solid var(--border); }}
  table.species tr:last-child td {{ border-bottom: 0; }}
  table.species th {{ background: #f3efe4; font-size: .8rem; text-transform: uppercase; letter-spacing: .04em; color: #4a3e1f; }}
  table.species td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  table.species em.bot {{ color: var(--muted); font-style: italic; font-weight: normal; font-size: .9em; }}
  ul.samples {{ list-style: none; padding: 0; }}
  ul.samples li {{ background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: .8rem 1rem; margin: .55rem 0; line-height: 1.5; }}
  ul.samples li .bot {{ color: var(--muted); font-style: italic; font-size: .92em; font-weight: 400; }}
  ul.samples li .meta {{ color: var(--muted); font-size: .9rem; }}
  ul.samples li .nbhd {{ color: #4a3e1f; }}
  ul.samples li a {{ color: var(--accent-dark); font-weight: 600; }}
  footer.site {{ border-top: 1px solid var(--rule); padding: 1.5rem 1.25rem 3rem; text-align: center; color: var(--muted); font-size: .85rem; max-width: 920px; margin: 3rem auto 0; }}
  footer.site a {{ color: var(--muted); }}
  @media (max-width: 520px) {{ .hero {{ margin: 1.5rem -1rem; }} main {{ padding: 1.25rem 1rem 2rem; }} }}
</style>
</head>
<body>

<header class="site">
  <div class="inner">
    <a class="home" href="../../">🌳 torontotrees</a>
    <nav class="mainnav"><a href="../../blog/">Blog</a><a href="../../calendar/">Calendar</a><a href="../../walks/">Walks</a><a href="../../neighbourhoods/">Neighbourhoods</a><a href="../">Lenses</a><a href="../../about/">About</a></nav>
  </div>
</header>

<main>
  <p class="kicker">🔍 Lens · {html.escape(lens['kicker'])}</p>
  <h1>{html.escape(title)}</h1>
  <p class="count"><strong>{n_matches:,}</strong> trees match this lens — {n_matches/689013*100:.1f}% of Toronto's catalogued street-tree inventory.</p>

  <div class="hero">
    <img src="../../charts/lens/{lens['slug']}.webp" alt="Map of Toronto's street trees with the {html.escape(lens['kicker'].lower())} highlighted in {lens['colour']} over a dimmed dot-map of every other tree in the city.">
  </div>

  <p class="intro">{html.escape(lens['intro'])}</p>

  <h2>Top species in this lens</h2>
  {species_table}

  <h2>Notable specimens</h2>
  <p style="color: var(--muted); font-size: .94rem; margin-bottom: .8rem;">A few of the largest matching trees, with permalinks to view each one on the map.</p>
  {sample_html}

  <p style="margin-top: 2rem"><a href="../">← Back to all lenses</a></p>
</main>

<footer class="site">
  Lens generated from City of Toronto <a href="https://open.toronto.ca/dataset/street-tree-data/">Street Tree Data</a>. Match counts and rendered map reflect the most recent monthly refresh.
</footer>

</body>
</html>
"""
    out = PAGES / lens["slug"] / "index.html"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(page)


def render_index_page(lens_summaries):
    """Gallery page listing every lens."""
    cards = []
    for lens, n_matches in lens_summaries:
        cards.append(f"""
        <a class="lens-card" href="{lens['slug']}/">
          <div class="thumb"><img src="../charts/lens/{lens['slug']}.webp" alt="" loading="lazy"></div>
          <div class="body">
            <h2>{html.escape(lens['title'])}</h2>
            <p class="kicker">{html.escape(lens['kicker'])}</p>
            <p class="count"><strong>{n_matches:,}</strong> trees · {n_matches/689013*100:.1f}% of inventory</p>
          </div>
        </a>""")

    page = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Lenses — curated views into Toronto's street-tree data | torontotrees</title>
<meta name="description" content="Pre-curated filter lenses into Toronto's 689,013 street trees. Veterans, native pollinator hosts, showy spring bloomers, citywide rarities. Each lens is a one-click visual answer to a specific question about the urban forest.">
<link rel="icon" href="data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'><text y='52' font-size='54'>🔍</text></svg>">
<meta property="og:title" content="Lenses — curated views into Toronto's street-tree data">
<meta property="og:description" content="Veterans, pollinator hosts, spring bloomers, rarities — pre-built filter views over Toronto's street trees.">
<meta property="og:type" content="website">
<style>
  :root {{ --fg:#1a1a1a; --muted:#666; --accent:#2b7a3d; --accent-dark:#1e5b2d; --bg:#faf7f2; --card:#fff; --border:#e2e2e2; --rule:#d9d4c7; }}
  * {{ box-sizing: border-box; }}
  body {{ font: 17px/1.65 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; background: var(--bg); color: var(--fg); -webkit-font-smoothing: antialiased; }}
  header.site {{ border-bottom: 1px solid var(--rule); padding: 1rem 1.25rem; }}
  header.site .inner {{ max-width: 920px; margin: 0 auto; display: flex; justify-content: space-between; align-items: center; gap: 1rem 1.5rem; flex-wrap: wrap; }}
  header.site a.home {{ color: var(--accent-dark); text-decoration: none; font-weight: 600; }}
  header.site a.home::before {{ content: "← "; opacity: 0.55; }}
  header.site nav.mainnav {{ display: flex; gap: 1.1rem; font-size: .9rem; flex-wrap: wrap; }}
  header.site nav.mainnav a {{ color: #666; text-decoration: none; }}
  header.site nav.mainnav a:hover {{ color: var(--accent-dark); text-decoration: underline; }}
  main {{ max-width: 920px; margin: 0 auto; padding: 2rem 1.25rem 3rem; }}
  h1 {{ font-size: clamp(1.7rem, 4vw, 2.3rem); margin: .25rem 0 .4rem; }}
  .intro {{ color: var(--muted); margin-bottom: 1.5rem; max-width: 700px; }}
  .lens-card {{ display: grid; grid-template-columns: 240px 1fr; gap: 1rem; background: var(--card); border: 1px solid var(--border); border-radius: 10px; overflow: hidden; text-decoration: none; color: inherit; margin: 1rem 0; transition: transform .12s ease, box-shadow .12s ease; }}
  .lens-card:hover {{ transform: translateY(-2px); box-shadow: 0 6px 22px rgba(0,0,0,.08); }}
  .lens-card .thumb {{ background: #0b0d10; }}
  .lens-card .thumb img {{ width: 100%; height: 100%; object-fit: cover; display: block; }}
  .lens-card .body {{ padding: 1rem 1.2rem; }}
  .lens-card h2 {{ font-size: 1.15rem; margin: 0 0 .25rem; line-height: 1.25; }}
  .lens-card .kicker {{ color: var(--accent-dark); font-size: .85rem; margin: 0 0 .5rem; }}
  .lens-card .count {{ color: var(--muted); font-size: .92rem; margin: 0; }}
  .lens-card .count strong {{ color: var(--fg); }}
  footer.site {{ border-top: 1px solid var(--rule); padding: 1.5rem 1.25rem 3rem; text-align: center; color: var(--muted); font-size: .85rem; max-width: 920px; margin: 3rem auto 0; }}
  footer.site a {{ color: var(--muted); }}
  @media (max-width: 600px) {{ .lens-card {{ grid-template-columns: 1fr; }} .lens-card .thumb img {{ aspect-ratio: 16 / 8; }} }}
</style>
</head>
<body>

<header class="site">
  <div class="inner">
    <a class="home" href="../">🌳 torontotrees</a>
    <nav class="mainnav"><a href="../blog/">Blog</a><a href="../calendar/">Calendar</a><a href="../walks/">Walks</a><a href="../neighbourhoods/">Neighbourhoods</a><a href="../about/">About</a></nav>
  </div>
</header>

<main>
  <h1>🔍 Lenses</h1>
  <p class="intro">Pre-curated views into the City of Toronto's 689,013 street-tree inventory. Each lens answers one question with one map: which trees are the elders, which feed the food web, which throw the spring shows, and which are rare enough to be worth a walk.</p>

  {''.join(cards)}

  <p style="margin-top: 2rem; color: var(--muted); font-size: .9rem;">Want something more specific? <a href="../">Search by address or species from the home page</a>.</p>
</main>

<footer class="site">
  Lenses generated from <a href="https://open.toronto.ca/dataset/street-tree-data/">Street Tree Data</a>. Updated alongside the rest of the site each monthly rebuild.
</footer>

</body>
</html>
"""
    (PAGES / "index.html").write_text(page)


def main() -> None:
    con = duckdb.connect()
    summaries = []
    for lens in LENSES:
        print(f"=== {lens['slug']} ===")
        n_matches, _ = render_lens_png(con, lens)
        notable, species = render_lens_data(con, lens)
        render_lens_page(lens, n_matches, notable, species)
        print(f"  {n_matches:,} matches · {len(notable)} notable · {len(species)} species")
        summaries.append((lens, n_matches))
    render_index_page(summaries)
    print(f"wrote {len(LENSES)} lens pages + index to {PAGES.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
