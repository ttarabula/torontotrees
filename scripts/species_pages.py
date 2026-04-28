"""Generate per-species static pages with rich og:image cards.

For each of ~60 curated species (the calendar.json roster, which covers ~91%
of all city street trees), render:
  - site/charts/species/<slug>.webp   — dot-map of this species citywide (small)
  - site/charts/og/species/<slug>.png — 1200×630 social card
  - site/species/<slug>/index.html    — full species page

The HTML pages are static, search-engine-indexable, share well on social
networks (because each has a bespoke og:image). They duplicate some of the
content the homepage's JS species view shows, but with the addition of
SEO-friendly URLs, social cards, and per-species blog cross-links.

Generates a /species/index.html gallery listing all of them.
"""
from __future__ import annotations
from pathlib import Path
import html
import json
import re

import duckdb
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent.parent
PROC = ROOT / "data" / "processed"
SITE = ROOT / "site"
CHART_DIR = SITE / "charts" / "species_pages"
OG_DIR = SITE / "charts" / "og" / "species"
PAGE_DIR = SITE / "species"
CHART_DIR.mkdir(parents=True, exist_ok=True)
OG_DIR.mkdir(parents=True, exist_ok=True)
PAGE_DIR.mkdir(parents=True, exist_ok=True)

LAT_MIN, LAT_MAX = 43.58, 43.86
LON_MIN, LON_MAX = -79.65, -79.11
BG_DARK = "#0b0d10"
DIM = "#2a3a36"

# Cross-links from species → blog post (where the post is genus- or species-specific).
BLOG_FOR_SPECIES = {
    "acer platanoides": ("../../blog/norway-maple-paradox/", "The Norway-maple paradox"),
    "gleditsia triacanthos": ("../../blog/honey-locust/", "The honey locust — Toronto's civic workhorse"),
    "ginkgo biloba": ("../../blog/ginkgo/", "The ginkgo — Toronto's 17,474 living fossils"),
    "robinia pseudoacacia": ("../../blog/black-locust/", "The black locust — Toronto's 2,943 Robinia pseudoacacia"),
}


def slug(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[.']", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s


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


def render_dot_map(con, key: str, slug_: str, colour: str) -> int:
    """Citywide dot-map: this species in colour, the rest dim."""
    ctx = con.execute(f"""
        SELECT lat, lon FROM read_parquet('{PROC}/trees.parquet')
        WHERE lat BETWEEN {LAT_MIN} AND {LAT_MAX}
          AND lon BETWEEN {LON_MIN} AND {LON_MAX}
        USING SAMPLE 50000
    """).fetchdf()
    matches = con.execute(f"""
        SELECT lat, lon FROM read_parquet('{PROC}/trees.parquet')
        WHERE lat BETWEEN {LAT_MIN} AND {LAT_MAX}
          AND lon BETWEEN {LON_MIN} AND {LON_MAX}
          AND botanical_key = '{key}'
    """).fetchdf()
    n = len(matches)

    fig, ax = plt.subplots(figsize=(11, 6.5), dpi=110)
    fig.patch.set_facecolor(BG_DARK)
    ax.set_facecolor(BG_DARK)
    ax.scatter(ctx["lon"], ctx["lat"], s=0.18, c=DIM, alpha=0.6, linewidths=0)
    ax.scatter(matches["lon"], matches["lat"], s=0.7, c=colour, alpha=0.85, linewidths=0)
    ax.set_xlim(LON_MIN, LON_MAX)
    ax.set_ylim(LAT_MIN, LAT_MAX)
    ax.set_aspect(1 / 0.72)
    ax.axis("off")
    fig.tight_layout(pad=0)
    fig.savefig(CHART_DIR / f"{slug_}.webp", facecolor=BG_DARK,
                bbox_inches="tight", pad_inches=0.05,
                pil_kwargs={"quality": 80, "method": 6})
    plt.close(fig)
    return n


def render_og_card(common: str, botanical: str, n: int, slug_: str, colour: str, con, key: str):
    """1200×630 social card. Big species name + count + small distribution map."""
    fig = plt.figure(figsize=(12, 6.3), dpi=100)
    fig.patch.set_facecolor("#faf7f2")

    # Left half: text. Right half: dot-map.
    ax_text = fig.add_axes([0.05, 0.05, 0.55, 0.9])
    ax_text.axis("off")
    ax_text.set_xlim(0, 1)
    ax_text.set_ylim(0, 1)
    ax_text.text(0, 0.85, "treeto.ca", fontsize=15, color="#1e5b2d", fontweight="bold", family="sans-serif")
    ax_text.text(0, 0.65, common, fontsize=46, color="#1a1a1a", fontweight="bold", family="sans-serif",
                 wrap=True, va="center")
    ax_text.text(0, 0.42, botanical, fontsize=22, color="#666", style="italic", family="sans-serif", va="center")
    ax_text.text(0, 0.18, f"{n:,}", fontsize=44, color=colour, fontweight="bold", family="sans-serif", va="center")
    ax_text.text(0, 0.06, "in Toronto's street-tree inventory", fontsize=14, color="#666",
                 family="sans-serif", va="center")

    # Right half: dot map
    ax_map = fig.add_axes([0.62, 0.05, 0.35, 0.9])
    ax_map.set_facecolor(BG_DARK)
    ctx = con.execute(f"""
        SELECT lat, lon FROM read_parquet('{PROC}/trees.parquet')
        WHERE lat BETWEEN {LAT_MIN} AND {LAT_MAX}
          AND lon BETWEEN {LON_MIN} AND {LON_MAX}
        USING SAMPLE 30000
    """).fetchdf()
    matches = con.execute(f"""
        SELECT lat, lon FROM read_parquet('{PROC}/trees.parquet')
        WHERE lat BETWEEN {LAT_MIN} AND {LAT_MAX}
          AND lon BETWEEN {LON_MIN} AND {LON_MAX}
          AND botanical_key = '{key}'
    """).fetchdf()
    ax_map.scatter(ctx["lon"], ctx["lat"], s=0.15, c=DIM, alpha=0.55, linewidths=0)
    ax_map.scatter(matches["lon"], matches["lat"], s=0.9, c=colour, alpha=0.85, linewidths=0)
    ax_map.set_xlim(LON_MIN, LON_MAX)
    ax_map.set_ylim(LAT_MIN, LAT_MAX)
    ax_map.set_aspect(1 / 0.72)
    ax_map.axis("off")

    fig.savefig(OG_DIR / f"{slug_}.png", facecolor="#faf7f2",
                bbox_inches="tight", pad_inches=0.1, pil_kwargs={"optimize": True})
    plt.close(fig)


def render_species_page(species, key, slug_, n, top_nbhds, notable, info, to_attrs, lineage, blog_link):
    common = species.get("common") or pretty_common(species.get("common"))
    botanical = species.get("botanical") or key

    pretty = pretty_common(common)
    desc = f"{n:,} {pretty.lower()}s on Toronto's streets — distribution, neighbourhoods, notable specimens, and bloom calendar."

    # Bloom & fall windows
    bloom_html = ""
    if species.get("bloom"):
        b = species["bloom"]
        m_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        bloom_html = f'<p class="window">🌸 <strong>Blooms</strong> {m_names[b["start"]["m"]-1]} {b["start"]["d"]} – {m_names[b["end"]["m"]-1]} {b["end"]["d"]}{": " + html.escape(b.get("note","")) if b.get("note") else ""}</p>'
    fall_html = ""
    if species.get("fall"):
        f = species["fall"]
        m_names = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
        fall_html = f'<p class="window">🍁 <strong>Fall colour</strong> {m_names[f["start"]["m"]-1]} {f["start"]["d"]} – {m_names[f["end"]["m"]-1]} {f["end"]["d"]}{": " + html.escape(f.get("note","")) if f.get("note") else ""}</p>'

    # City planting attributes from species_toronto.json
    facts_rows = []
    if to_attrs:
        for label, key2 in [
            ("Native to", "native"),
            ("Mature size", "size"),
            ("Growth rate", "growth"),
            ("Sensitivity", "sensitivity"),
            ("Best site", "location"),
            ("Of note", "other"),
        ]:
            v = to_attrs.get(key2)
            if v:
                facts_rows.append(f"<tr><td>{label}</td><td>{html.escape(str(v))}</td></tr>")
        if to_attrs.get("wires") is not None:
            facts_rows.append(f"<tr><td>Plants under overhead wires</td><td>{'Yes' if to_attrs['wires'] else 'No'}</td></tr>")
    facts_html = ""
    if facts_rows:
        facts_html = "<h2>Planting profile (from the City of Toronto)</h2><table class='facts'><tbody>" + "".join(facts_rows) + "</tbody></table>"

    # Lineage blurb
    lineage_html = ""
    if lineage:
        lineage_html = f'<div class="lineage"><strong>Toronto history —</strong> {html.escape(lineage)}</div>'

    # Wikipedia / iNat links
    ext_links = []
    if info.get("wiki_url"):
        ext_links.append(f'<a href="{html.escape(info["wiki_url"])}" target="_blank" rel="noopener">Wikipedia</a>')
    ext_links.append(f'<a href="https://www.inaturalist.org/search?q={html.escape(common)}" target="_blank" rel="noopener">iNaturalist</a>')

    extract_html = ""
    if info.get("extract"):
        extract_html = f'<p class="extract">{html.escape(info["extract"])}</p>'

    # Top neighbourhoods
    nbhd_html = ""
    if top_nbhds:
        rows = []
        for n_name, count in top_nbhds[:8]:
            ns = slug(n_name)
            rows.append(f"<tr><td><a href='../../neighbourhoods/{ns}/'>{html.escape(n_name)}</a></td><td class='num'>{count:,}</td></tr>")
        nbhd_html = "<h2>Where they cluster</h2><table class='nbhds'><thead><tr><th>Neighbourhood</th><th class='num'>Trees</th></tr></thead><tbody>" + "".join(rows) + "</tbody></table>"

    # Notable specimens
    notable_html = ""
    if notable:
        items = []
        for t in notable[:8]:
            addr = " ".join(p for p in [
                clean_part(t.get("ADDRESS")), clean_part(t.get("STREETNAME")), clean_part(t.get("SUFFIX"))
            ] if p)
            dbh = int(t.get("dbh_cm") or 0)
            permalink = f"../../#@{float(t['lat']):.5f},{float(t['lon']):.5f}"
            items.append(f'<li><strong>{dbh} cm DBH</strong> at {html.escape(addr)} · <a href="{permalink}">view →</a></li>')
        notable_html = f"<h2>Notable specimens</h2><ul class='specimens'>{''.join(items)}</ul>"

    # Cross-link to blog
    blog_html = ""
    if blog_link:
        url, title = blog_link
        blog_html = f'<div class="cta"><p style="margin: 0 0 .25rem"><strong>Read more:</strong></p><a href="{url}">{html.escape(title)} →</a></div>'

    # OG title — fits the social card preview
    og_title = f"{pretty} ({botanical}) — {n:,} on Toronto's streets"

    page = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(pretty)} ({html.escape(botanical)}) — Toronto's street-tree inventory | torontotrees</title>
<meta name="description" content="{html.escape(desc)}">
<link rel="icon" href="data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'><text y='52' font-size='54'>🌳</text></svg>">
<meta property="og:title" content="{html.escape(og_title)}">
<meta property="og:description" content="{html.escape(desc)}">
<meta property="og:type" content="article">
<meta property="og:image" content="https://treeto.ca/charts/og/species/{slug_}.png">
<meta property="og:image:width" content="1200">
<meta property="og:image:height" content="630">
<meta name="twitter:card" content="summary_large_image">
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
  .kicker {{ color: var(--accent-dark); font-weight: 600; font-size: .9rem; letter-spacing: .02em; text-transform: uppercase; margin: 0 0 .4rem; }}
  h1 {{ font-size: clamp(1.7rem, 4vw, 2.3rem); line-height: 1.18; margin: .25rem 0 .15rem; }}
  .botanical {{ color: var(--muted); font-style: italic; font-size: 1.05rem; margin: 0 0 .75rem; }}
  .count {{ color: var(--muted); font-size: 1rem; margin-bottom: 1.5rem; }}
  .count strong {{ color: #1e5b2d; font-size: 1.2rem; }}
  .hero {{ margin: 1.5rem -1.25rem; }}
  .hero img {{ width: 100%; height: auto; display: block; background: #0b0d10; }}
  .window {{ background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: .7rem 1rem; margin: .35rem 0; font-size: .98rem; }}
  .lineage {{ background: #f3efe4; border-left: 3px solid var(--accent); padding: .8rem 1rem; border-radius: 0 6px 6px 0; margin: 1.5rem 0; line-height: 1.55; }}
  .extract {{ background: var(--card); border: 1px solid var(--border); padding: 1rem 1.2rem; border-radius: 8px; line-height: 1.6; color: #333; }}
  h2 {{ font-size: 1.2rem; margin: 2rem 0 .75rem; padding-top: 1.5rem; border-top: 1px solid var(--rule); }}
  table.facts, table.nbhds {{ width: 100%; max-width: 640px; border-collapse: collapse; margin: 1rem 0 1.5rem; font-size: .95rem; background: var(--card); border: 1px solid var(--border); border-radius: 6px; overflow: hidden; }}
  table.facts td, table.nbhds td, table.nbhds th {{ padding: .55rem .75rem; text-align: left; border-bottom: 1px solid var(--border); }}
  table.facts tr:last-child td, table.nbhds tr:last-child td {{ border-bottom: 0; }}
  table.facts td:first-child {{ background: #f3efe4; color: #4a3e1f; font-weight: 600; width: 35%; }}
  table.nbhds th {{ background: #f3efe4; color: #4a3e1f; font-size: .8rem; text-transform: uppercase; letter-spacing: .04em; }}
  table.nbhds td.num, table.nbhds th.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  ul.specimens {{ list-style: none; padding: 0; }}
  ul.specimens li {{ background: var(--card); border: 1px solid var(--border); border-radius: 6px; padding: .55rem .8rem; margin: .3rem 0; font-size: .96rem; }}
  ul.specimens a {{ color: var(--accent-dark); font-weight: 600; }}
  .links {{ margin: 1rem 0 1.5rem; }}
  .links a {{ margin-right: 1rem; color: var(--accent-dark); }}
  .cta {{ background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 1.25rem 1.5rem; margin: 2rem 0; text-align: center; }}
  .cta a {{ display: inline-block; padding: .6rem 1.2rem; background: var(--accent); color: white; text-decoration: none; border-radius: 6px; font-weight: 600; margin-top: .25rem; }}
  footer.site {{ border-top: 1px solid var(--rule); padding: 1.5rem 1.25rem 3rem; text-align: center; color: var(--muted); font-size: .85rem; max-width: 920px; margin: 3rem auto 0; }}
  footer.site a {{ color: var(--muted); }}
  @media (max-width: 520px) {{ .hero {{ margin: 1.5rem -1rem; }} main {{ padding: 1.25rem 1rem 2rem; }} }}
</style>
</head>
<body>

<header class="site">
  <div class="inner">
    <a class="home" href="../../">🌳 torontotrees</a>
    <nav class="mainnav"><a href="../../blog/">Blog</a><a href="../../calendar/">Calendar</a><a href="../../walks/">Walks</a><a href="../../neighbourhoods/">Neighbourhoods</a><a href="../../lenses/">Lenses</a><a href="../">Species</a></nav>
  </div>
</header>

<main>
  <p class="kicker">Species profile</p>
  <h1>{html.escape(pretty)}</h1>
  <p class="botanical">{html.escape(botanical)}</p>
  <p class="count"><strong>{n:,}</strong> on Toronto's streets — {n/689013*100:.2f}% of the city's catalogued canopy.</p>

  <div class="hero">
    <img src="../../charts/species_pages/{slug_}.webp" alt="Map of Toronto with every {html.escape(pretty.lower())} highlighted, over a dimmed dot-map of every other species in the city.">
  </div>

  {bloom_html}
  {fall_html}

  {lineage_html}

  {extract_html}

  <div class="links">{' · '.join(ext_links)}</div>

  {facts_html}

  {nbhd_html}

  {notable_html}

  {blog_html}

  <p style="margin-top: 2rem"><a href="../">← All species</a> · <a href="../../">Search any address</a></p>
</main>

<footer class="site">
  Data: <a href="https://open.toronto.ca/dataset/street-tree-data/">Street Tree Data</a> (City of Toronto, OGL-Toronto) · Wikipedia summaries via the public API · planting attributes from the City's <a href="https://www.toronto.ca/services-payments/water-environment/trees/tree-planting/species-planted-on-streets/">Species planted on streets</a> page.
</footer>

</body>
</html>
"""
    out = PAGE_DIR / slug_ / "index.html"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(page)


def render_gallery_page(species_summaries):
    """Gallery listing every generated species page."""
    cards = []
    for sp, slug_, n in species_summaries:
        common = pretty_common(sp.get("common"))
        bot = sp.get("botanical", sp.get("key", ""))
        cards.append(f"""
        <a class="sp-card" href="{slug_}/">
          <div class="thumb"><img src="../charts/species_pages/{slug_}.webp" alt="" loading="lazy"></div>
          <div class="body">
            <h2>{html.escape(common)}</h2>
            <p class="bot">{html.escape(bot)}</p>
            <p class="count"><strong>{n:,}</strong> trees</p>
          </div>
        </a>""")

    page = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Species — every catalogued tree on Toronto's streets | torontotrees</title>
<meta name="description" content="Profiles for the {len(species_summaries)} most-common street-tree species in Toronto. Each species page includes a citywide distribution map, top neighbourhoods, notable specimens, bloom and fall calendar windows, and the City of Toronto's planting profile.">
<link rel="icon" href="data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'><text y='52' font-size='54'>🌳</text></svg>">
<meta property="og:title" content="Toronto street-tree species — full profiles">
<meta property="og:description" content="60 species, with maps, notable specimens, and the City's planting profile each.">
<meta property="og:type" content="website">
<style>
  :root {{ --fg:#1a1a1a; --muted:#666; --accent:#2b7a3d; --accent-dark:#1e5b2d; --bg:#faf7f2; --card:#fff; --border:#e2e2e2; --rule:#d9d4c7; }}
  * {{ box-sizing: border-box; }}
  body {{ font: 17px/1.65 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; background: var(--bg); color: var(--fg); -webkit-font-smoothing: antialiased; }}
  header.site {{ border-bottom: 1px solid var(--rule); padding: 1rem 1.25rem; }}
  header.site .inner {{ max-width: 1100px; margin: 0 auto; display: flex; justify-content: space-between; align-items: center; gap: 1rem 1.5rem; flex-wrap: wrap; }}
  header.site a.home {{ color: var(--accent-dark); text-decoration: none; font-weight: 600; }}
  header.site a.home::before {{ content: "← "; opacity: 0.55; }}
  header.site nav.mainnav {{ display: flex; gap: 1.1rem; font-size: .9rem; flex-wrap: wrap; }}
  header.site nav.mainnav a {{ color: #666; text-decoration: none; }}
  header.site nav.mainnav a:hover {{ color: var(--accent-dark); text-decoration: underline; }}
  main {{ max-width: 1100px; margin: 0 auto; padding: 2rem 1.25rem 3rem; }}
  h1 {{ font-size: clamp(1.7rem, 4vw, 2.2rem); margin: .25rem 0 .4rem; }}
  .intro {{ color: var(--muted); margin-bottom: 1.5rem; max-width: 700px; }}
  .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); gap: 1rem; }}
  .sp-card {{ background: var(--card); border: 1px solid var(--border); border-radius: 10px; overflow: hidden; text-decoration: none; color: inherit; transition: transform .12s ease, box-shadow .12s ease; }}
  .sp-card:hover {{ transform: translateY(-2px); box-shadow: 0 6px 22px rgba(0,0,0,.08); }}
  .sp-card .thumb {{ background: #0b0d10; aspect-ratio: 16 / 10; overflow: hidden; }}
  .sp-card .thumb img {{ width: 100%; height: 100%; object-fit: cover; display: block; }}
  .sp-card .body {{ padding: .85rem 1rem 1rem; }}
  .sp-card h2 {{ font-size: 1rem; margin: 0 0 .15rem; }}
  .sp-card .bot {{ color: var(--muted); font-style: italic; font-size: .85rem; margin: 0 0 .35rem; }}
  .sp-card .count {{ color: var(--muted); font-size: .88rem; margin: 0; }}
  .sp-card .count strong {{ color: var(--fg); }}
  footer.site {{ border-top: 1px solid var(--rule); padding: 1.5rem 1.25rem 3rem; text-align: center; color: var(--muted); font-size: .85rem; max-width: 1100px; margin: 3rem auto 0; }}
  footer.site a {{ color: var(--muted); }}
</style>
</head>
<body>

<header class="site">
  <div class="inner">
    <a class="home" href="../">🌳 torontotrees</a>
    <nav class="mainnav"><a href="../blog/">Blog</a><a href="../calendar/">Calendar</a><a href="../walks/">Walks</a><a href="../neighbourhoods/">Neighbourhoods</a><a href="../lenses/">Lenses</a><a href="../about/">About</a></nav>
  </div>
</header>

<main>
  <h1>🌳 Species</h1>
  <p class="intro">Profiles for the {len(species_summaries)} most-common street-tree species in Toronto, ranked by abundance. Together they cover roughly 91% of the catalogued inventory. Each page has a citywide distribution map, the top neighbourhoods, notable specimens with permalinks, the City's planting profile, and bloom / fall-colour windows.</p>

  <div class="grid">
    {''.join(cards)}
  </div>

  <p style="margin-top: 2.5rem; color: var(--muted); font-size: .9rem;">Looking for something else? <a href="../">Search any address or species from the home page</a> · <a href="../lenses/">Browse curated lenses</a></p>
</main>

<footer class="site">
  Data: <a href="https://open.toronto.ca/dataset/street-tree-data/">Street Tree Data</a> · Wikipedia summaries via the public API · planting attributes from the <a href="https://www.toronto.ca/services-payments/water-environment/trees/tree-planting/species-planted-on-streets/">City of Toronto's Species planted on streets</a>.
</footer>

</body>
</html>
"""
    (PAGE_DIR / "index.html").write_text(page)


def main() -> None:
    con = duckdb.connect()
    calendar = json.loads((SITE / "data" / "calendar.json").read_text())
    species_data = json.loads((SITE / "data" / "species.json").read_text())
    to_attrs_all = json.loads((SITE / "data" / "species_toronto.json").read_text())
    lineage_all = json.loads((SITE / "data" / "species_lineage.json").read_text())

    summaries = []
    for sp in calendar:
        key = sp["key"]
        slug_ = slug(key)
        # Pick a species-typical colour from the bloom/fall tags
        tags = sp.get("tags") or []
        if "showy_bloom" in tags or "fragrant_bloom" in tags:
            colour = "#c6256b"
        elif "showy_fall" in tags:
            colour = "#d2651d"
        else:
            colour = "#6bb76b"

        # Species count + dot map
        n = render_dot_map(con, key, slug_, colour)
        if n == 0:
            print(f"  skip {key}: 0 trees")
            continue

        # Top nbhds for this species
        top_q = f"""
        SELECT nbhd_name, COUNT(*) n FROM read_parquet('{PROC}/trees.parquet')
        WHERE botanical_key = '{key}' AND nbhd_name IS NOT NULL
        GROUP BY 1 ORDER BY 2 DESC LIMIT 8
        """
        top_nbhds = list(con.execute(top_q).fetchall())

        # Notable specimens (largest, capped at 200cm)
        notable = con.execute(f"""
        SELECT _id, ADDRESS, STREETNAME, SUFFIX, dbh_cm, lat, lon
        FROM read_parquet('{PROC}/trees.parquet')
        WHERE botanical_key = '{key}' AND dbh_cm BETWEEN 1 AND 200
        ORDER BY dbh_cm DESC LIMIT 8
        """).fetchdf().to_dict(orient="records")

        info = species_data.get(key, {})
        to_attrs = to_attrs_all.get(key)
        lineage = lineage_all.get(key)
        blog_link = BLOG_FOR_SPECIES.get(key)

        # Render the OG card
        botanical = sp.get("botanical", key)
        common_pretty = pretty_common(sp.get("common"))
        render_og_card(common_pretty, botanical, n, slug_, colour, con, key)

        # Render the species page
        render_species_page(sp, key, slug_, n, top_nbhds, notable, info, to_attrs, lineage, blog_link)

        summaries.append((sp, slug_, n))
        print(f"  {key}: {n:,} trees → /species/{slug_}/")

    # Sort gallery by abundance
    summaries.sort(key=lambda x: x[2], reverse=True)
    render_gallery_page(summaries)
    print(f"\nwrote {len(summaries)} species pages + gallery to {PAGE_DIR.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
