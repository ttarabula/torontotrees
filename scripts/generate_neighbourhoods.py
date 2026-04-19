"""Generate a page per Toronto neighbourhood (158) + one index page.

Each page: stats, rank-context, top species, narrative, cross-links.
Reads the pre-computed parquets in data/processed/ and writes HTML to
site/neighbourhoods/.

Idempotent — safe to re-run after underlying data changes.
"""
from pathlib import Path
import html
import json
import re

import duckdb

ROOT = Path(__file__).resolve().parent.parent
PROC = ROOT / "data" / "processed"
OUT = ROOT / "site" / "neighbourhoods"


def slug(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[.']", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s


def fmt_int(n):
    return f"{int(round(n)):,}"


def pretty_common(name: str) -> str:
    """'Oak, red' -> 'Red Oak'; 'Pear Chanticleer' -> 'Pear Chanticleer'."""
    if not name:
        return "—"
    name = name.strip()
    if "," in name:
        family, cultivar = [p.strip() for p in name.split(",", 1)]
        return f"{cultivar} {family}".title()
    return name.title()


def clean_suffix(p):
    """Drop pandas NA / 'None' / 'nan' strings."""
    s = str(p or "").strip()
    if s.lower() in {"", "none", "nan"}:
        return ""
    return s


def fmt_pct(x, digits=1):
    return f"{x:.{digits}f}%"


def ordinal(n: int) -> str:
    if 10 <= n % 100 <= 20:
        suf = "th"
    else:
        suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suf}"


def load_data():
    con = duckdb.connect()
    # Merge per-nbhd tables
    merged = con.execute(f"""
        SELECT s.nbhd_code, s.nbhd_name, s.classification,
               s.area_km2, s.tree_count, s.trees_per_km2,
               s.trees_per_capita, s.median_hh_income, s.population_2021,
               s.species_count, s.shannon_h,
               s.top_species, s.top_species_share,
               c.canopy_pct,
               ch.canopy_2008, ch.canopy_2018, ch.change as canopy_change_pp,
               h.heat_proxy, h.impervious_pct,
               v.total_usd, v.avg_per_tree_usd, v.usd_per_capita
        FROM read_parquet('{PROC}/nbhd_summary.parquet') s
        LEFT JOIN read_parquet('{PROC}/nbhd_canopy.parquet') c ON s.nbhd_name = c.nbhd_name
        LEFT JOIN read_parquet('{PROC}/nbhd_canopy_change.parquet') ch ON s.nbhd_name = ch.nbhd_name
        LEFT JOIN read_parquet('{PROC}/nbhd_heat.parquet') h ON s.nbhd_name = h.nbhd_name
        LEFT JOIN read_parquet('{PROC}/nbhd_value.parquet') v ON s.nbhd_name = v.nbhd_name
    """).fetchdf()

    # Top-5 species per neighbourhood (botanical keys + counts)
    top = con.execute(f"""
        WITH ranked AS (
            SELECT nbhd_code, nbhd_name, botanical_key, common_key,
                   COUNT(*) AS n,
                   ROW_NUMBER() OVER (PARTITION BY nbhd_code ORDER BY COUNT(*) DESC) AS rk
            FROM read_parquet('{PROC}/trees.parquet')
            WHERE nbhd_code IS NOT NULL
            GROUP BY nbhd_code, nbhd_name, botanical_key, common_key
        )
        SELECT nbhd_code, nbhd_name, botanical_key, common_key, n
        FROM ranked WHERE rk <= 5
        ORDER BY nbhd_code, rk
    """).fetchdf()

    # Biggest tree per neighbourhood (by DBH). Cap at 200cm to exclude data-entry
    # errors — Toronto's genuinely-largest street trees top out around 180cm.
    biggest = con.execute(f"""
        WITH ranked AS (
            SELECT nbhd_code, ADDRESS, STREETNAME, SUFFIX, botanical_key, common_key,
                   dbh_cm, lat, lon,
                   ROW_NUMBER() OVER (PARTITION BY nbhd_code ORDER BY dbh_cm DESC NULLS LAST) AS rk
            FROM read_parquet('{PROC}/trees.parquet')
            WHERE nbhd_code IS NOT NULL AND dbh_cm IS NOT NULL AND dbh_cm <= 200
        )
        SELECT nbhd_code, ADDRESS, STREETNAME, SUFFIX, botanical_key, common_key, dbh_cm, lat, lon
        FROM ranked WHERE rk = 1
    """).fetchdf()

    return merged, top, biggest


def pct_rank(values, x, higher_is_better=True):
    """Percentile rank of x within values (0–100)."""
    n = len(values)
    below = sum(1 for v in values if v < x)
    if not higher_is_better:
        below = sum(1 for v in values if v > x)
    return round(100 * below / (n - 1), 0) if n > 1 else 50


def rank_of(values_sorted_desc, x):
    """1-indexed rank of x in descending sort. Handles ties by position."""
    return values_sorted_desc.index(x) + 1 if x in values_sorted_desc else None


def page_head(title, desc, depth):
    back = "../" * depth
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(title)} — torontotrees</title>
<meta name="description" content="{html.escape(desc)}">
<link rel="icon" href="data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 64 64'><text y='52' font-size='54'>🌳</text></svg>">
<meta property="og:title" content="{html.escape(title)}">
<meta property="og:description" content="{html.escape(desc)}">
<meta property="og:type" content="article">
<style>
  :root {{
    --fg: #1a1a1a; --muted: #666;
    --accent: #2b7a3d; --accent-dark: #1e5b2d;
    --bg: #faf7f2; --card: #fff;
    --border: #e2e2e2; --rule: #d9d4c7;
  }}
  * {{ box-sizing: border-box; }}
  html {{ -webkit-text-size-adjust: 100%; }}
  body {{
    font: 17px/1.65 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    margin: 0; background: var(--bg); color: var(--fg);
    -webkit-font-smoothing: antialiased;
  }}
  header.site {{ border-bottom: 1px solid var(--rule); padding: 1rem 1.25rem; background: var(--bg); }}
  header.site .inner {{ max-width: 900px; margin: 0 auto; display: flex; justify-content: space-between; align-items: baseline; gap: 1rem; flex-wrap: wrap; }}
  header.site a.home {{ color: var(--accent-dark); text-decoration: none; font-weight: 600; }}
  header.site a.home:hover {{ text-decoration: underline; }}
  header.site .mainnav a {{ color: var(--accent-dark); text-decoration: none; margin-left: 1rem; font-size: .95rem; }}
  header.site .mainnav a:hover {{ text-decoration: underline; }}
  main {{ max-width: 780px; margin: 0 auto; padding: 2rem 1.25rem 3rem; }}
  main .kicker {{ color: var(--accent-dark); font-weight: 600; font-size: .9rem; letter-spacing: .02em; text-transform: uppercase; margin: 0 0 .4rem; }}
  main h1 {{ font-size: clamp(1.8rem, 4.5vw, 2.4rem); line-height: 1.15; margin: 0 0 .4rem; }}
  main .subhead {{ color: var(--muted); font-size: 1rem; margin-bottom: 2rem; }}
  main h2 {{ font-size: 1.25rem; margin: 2.2rem 0 .75rem; padding-top: 1.5rem; border-top: 1px solid var(--rule); }}
  main p {{ margin: 0 0 1.1em; }}
  main a {{ color: var(--accent-dark); }}
  main strong {{ color: #111; }}
  .stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: .75rem; margin: 1.5rem 0; }}
  .stat {{ background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: .8rem .9rem; }}
  .stat .label {{ color: var(--muted); font-size: .8rem; text-transform: uppercase; letter-spacing: .04em; }}
  .stat .val {{ font-size: 1.3rem; font-weight: 600; margin-top: .2rem; font-variant-numeric: tabular-nums; }}
  .stat .rank {{ color: var(--muted); font-size: .82rem; margin-top: .15rem; }}
  .species {{ border-collapse: collapse; width: 100%; margin: .75rem 0 1.5rem; background: var(--card); border: 1px solid var(--border); border-radius: 6px; overflow: hidden; }}
  .species th, .species td {{ padding: .5rem .8rem; text-align: left; border-bottom: 1px solid #f0f0f0; }}
  .species tr:last-child td {{ border-bottom: 0; }}
  .species th {{ background: #f3efe4; color: #4a3e1f; font-size: .78rem; text-transform: uppercase; letter-spacing: .04em; }}
  .species td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  .species .bar {{ display: inline-block; height: .55rem; background: var(--accent); border-radius: 2px; vertical-align: middle; }}
  .species .bot {{ font-style: italic; color: var(--muted); font-size: .88rem; }}
  .chip {{ display: inline-block; background: #eef3ed; color: var(--accent-dark); padding: .15rem .55rem; border-radius: 999px; font-size: .78rem; margin-right: .3rem; }}
  .chip.warn {{ background: #fdecd6; color: #8a4b0b; }}
  .note {{ color: var(--muted); font-size: .86rem; margin-top: 2rem; border-top: 1px solid var(--rule); padding-top: 1rem; }}
  .crosslinks {{ display: flex; gap: .75rem; flex-wrap: wrap; margin-top: .5rem; }}
  .crosslinks a {{ background: var(--card); border: 1px solid var(--border); padding: .5rem .8rem; border-radius: 8px; text-decoration: none; }}
  .crosslinks a:hover {{ background: #eef3ed; border-color: var(--accent); }}
  table.ranking {{ width: 100%; border-collapse: collapse; font-size: .95rem; margin-top: 1rem; }}
  table.ranking th, table.ranking td {{ padding: .55rem .65rem; text-align: left; border-bottom: 1px solid #f0f0f0; }}
  table.ranking th {{ background: #f3efe4; font-size: .78rem; text-transform: uppercase; letter-spacing: .04em; cursor: pointer; user-select: none; }}
  table.ranking th:hover {{ background: #e9dfc9; }}
  table.ranking th .arrow {{ color: var(--muted); font-size: .7rem; }}
  table.ranking td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  table.ranking td a {{ color: var(--accent-dark); text-decoration: none; font-weight: 500; }}
  table.ranking td a:hover {{ text-decoration: underline; }}
  .filter {{ margin: 1rem 0; }}
  .filter input {{ width: 100%; max-width: 400px; padding: .5rem .8rem; border: 1px solid var(--border); border-radius: 6px; font-size: 1rem; }}
</style>
</head>
<body>
<header class="site">
  <div class="inner">
    <a class="home" href="{back}">🌳 torontotrees</a>
    <nav class="mainnav"><a href="{back}blog/">Blog</a><a href="{back}calendar/">Calendar</a><a href="{back}walks/">Walks</a><a href="{back}neighbourhoods/">Neighbourhoods</a><a href="{back}about/">About</a></nav>
  </div>
</header>

<main>
"""


def page_foot():
    return """
</main>
</body>
</html>
"""


NBHD_NARRATIVE_TEMPLATE = """
<p>{first_line}</p>
<p>{canopy_line} {heat_line}</p>
<p>{species_line}</p>
"""


def narrative(row, ranks):
    dr = ranks["density_rank"]  # 1 = densest
    cr = ranks["canopy_rank"]
    ir = ranks["income_rank"]
    sr = ranks["shannon_rank"]
    total = ranks["total"]

    # density framing
    if dr <= 20:
        d_framing = f"one of the most tree-dense neighbourhoods in Toronto — {ordinal(dr)} of {total}"
    elif dr <= 60:
        d_framing = f"above-average for tree density ({ordinal(dr)} of {total})"
    elif dr >= total - 20:
        d_framing = f"among the least-forested in the city ({ordinal(dr)} of {total})"
    else:
        d_framing = f"middle-of-the-pack for street trees ({ordinal(dr)} of {total})"

    first = (
        f"<strong>{html.escape(row['nbhd_name'])}</strong> is {d_framing}, "
        f"with <strong>{fmt_int(row['tree_count'])}</strong> city-owned street trees "
        f"across <strong>{row['area_km2']:.2f} km²</strong> — "
        f"{fmt_int(row['trees_per_km2'])} per km²."
    )

    # canopy context
    canopy_line = ""
    cp = row.get("canopy_pct")
    if cp is not None and not (cp != cp):  # NaN guard
        change = row.get("canopy_change_pp")
        change_phrase = ""
        if change is not None and not (change != change):
            if change <= -1:
                change_phrase = f" — down {abs(change):.1f} points since 2008"
            elif change >= 1:
                change_phrase = f" — up {change:.1f} points since 2008"
            else:
                change_phrase = " — essentially unchanged since 2008"
        canopy_line = (
            f"Tree canopy covers <strong>{cp:.1f}%</strong> of the neighbourhood ({ordinal(cr)} of {total}){change_phrase}. "
            "(This includes all trees — street, park, and private — from the 2018 land-cover raster.)"
        )

    # heat
    heat_line = ""
    hp = row.get("heat_proxy")
    if hp is not None and not (hp != hp):
        hr = ranks["heat_rank"]
        if hr <= 25:
            heat_line = f"Heat-risk proxy ranks it <strong>{ordinal(hr)}-hottest</strong> in the city."
        elif hr >= total - 25:
            heat_line = f"Heat-risk proxy ranks it <strong>{ordinal(total - hr + 1)}-coolest</strong> in the city."

    # species / diversity
    species_line = (
        f"Across <strong>{row['species_count']}</strong> distinct species (Shannon diversity "
        f"{row['shannon_h']:.2f}, {ordinal(sr)} of {total}), "
        f"the most common is <em>{html.escape(row['top_species'])}</em> at "
        f"<strong>{row['top_species_share']*100:.1f}%</strong> of the trees."
    )
    if row["top_species_share"] >= 0.25:
        species_line += " <span class=\"chip warn\">Monoculture watch</span>"

    return NBHD_NARRATIVE_TEMPLATE.format(
        first_line=first,
        canopy_line=canopy_line,
        heat_line=heat_line,
        species_line=species_line,
    )


def species_table(species_rows, total_trees):
    if not len(species_rows):
        return ""
    max_n = species_rows["n"].max()
    parts = ["<table class='species'><thead><tr><th>Species</th><th class='num'>Trees</th><th class='num'>Share</th></tr></thead><tbody>"]
    for _, r in species_rows.iterrows():
        share = r["n"] / total_trees
        bar_w = round(100 * r["n"] / max_n)
        common = pretty_common(r["common_key"])
        bot = r["botanical_key"] or ""
        parts.append(
            f"<tr><td>{html.escape(common)} <span class='bot'>{html.escape(bot)}</span></td>"
            f"<td class='num'>{fmt_int(r['n'])}</td>"
            f"<td class='num'><span class='bar' style='width:{bar_w * 0.5}px'></span> {share*100:.1f}%</td></tr>"
        )
    parts.append("</tbody></table>")
    return "".join(parts)


def biggest_tree_block(big_row, total_trees):
    if big_row is None:
        return ""
    dbh = int(big_row.get("dbh_cm") or 0)
    # Sanity filter: Toronto's largest genuine street trees are ~180cm DBH.
    # Values >200cm are almost certainly data entry errors (a 250cm/2.5m
    # diameter oak would be North America's largest oak).
    if dbh < 10 or dbh > 200:
        return ""
    addr_parts = [clean_suffix(big_row.get("ADDRESS")), clean_suffix(big_row.get("STREETNAME")), clean_suffix(big_row.get("SUFFIX"))]
    addr = " ".join(p for p in addr_parts if p)
    common = pretty_common(big_row.get("common_key"))
    bot = big_row.get("botanical_key") or ""
    lat, lon = big_row.get("lat"), big_row.get("lon")
    sv_url = f"https://www.google.com/maps/@?api=1&map_action=pano&viewpoint={lat},{lon}" if lat and lon else None
    sv = f' · <a href="{sv_url}" target="_blank" rel="noopener">Street View</a>' if sv_url else ""
    return (
        f"<h2>The biggest tree on record</h2>"
        f"<p>A <strong>{html.escape(common)}</strong> (<em>{html.escape(bot)}</em>) at "
        f"<strong>{html.escape(addr)}</strong> — <strong>{dbh} cm DBH</strong>, the largest of the {fmt_int(total_trees)} street trees here.{sv}</p>"
    )


def nbhd_page_html(row, species_rows, big_row, ranks):
    nb = row["nbhd_name"]
    desc = (
        f"{int(row['tree_count']):,} street trees across {row['area_km2']:.1f} km² — "
        f"top species, canopy %, density, and how {nb} ranks in Toronto."
    )
    title = f"{nb} — street trees"
    nia_chip = ""
    cls = str(row.get("classification") or "")
    if cls == "Neighbourhood Improvement Area":
        nia_chip = '<span class="chip warn">Neighbourhood Improvement Area</span> '
    elif cls == "Emerging Neighbourhood":
        nia_chip = '<span class="chip">Emerging Neighbourhood</span> '

    head = page_head(title, desc, depth=2)
    narr = narrative(row, ranks)

    species_html = species_table(species_rows, row["tree_count"])

    big_block = biggest_tree_block(big_row, row["tree_count"])

    # Stats grid
    stats = []

    def stat(label, val, rank=None, total=None, suffix=""):
        rank_html = f"<div class='rank'>{ordinal(int(rank))} of {total}</div>" if rank else ""
        return f"<div class='stat'><div class='label'>{label}</div><div class='val'>{val}{suffix}</div>{rank_html}</div>"

    total = ranks["total"]
    stats.append(stat("Street trees", fmt_int(row["tree_count"]), ranks["count_rank"], total))
    stats.append(stat("Trees per km²", fmt_int(row["trees_per_km2"]), ranks["density_rank"], total))
    if row.get("canopy_pct") and row["canopy_pct"] == row["canopy_pct"]:
        stats.append(stat("Canopy coverage", fmt_pct(row["canopy_pct"]), ranks["canopy_rank"], total))
    stats.append(stat("Species diversity (H)", f"{row['shannon_h']:.2f}", ranks["shannon_rank"], total))
    if row.get("total_usd") and row["total_usd"] == row["total_usd"]:
        stats.append(stat("Annual canopy value", f"${fmt_int(row['total_usd'])}", None, None, "/yr"))

    stats_html = f'<div class="stats">{"".join(stats)}</div>'

    # Cross-links
    cross = []
    cross.append('<a href="../../blog/canopy-equity/">Canopy equity post →</a>')
    if row.get("canopy_change_pp") is not None and abs(row["canopy_change_pp"]) >= 1:
        cross.append('<a href="../../blog/decade-of-standing-still/">Decade of standing still →</a>')
    if row.get("heat_proxy") is not None and ranks["heat_rank"] <= 25:
        cross.append('<a href="../../blog/heat-islands/">Heat-islands post →</a>')
    if row["top_species_share"] >= 0.20:
        cross.append('<a href="../../blog/honey-locust/">On monocultures →</a>')
    cross.append(f'<a href="../">← All neighbourhoods</a>')
    cross_html = f'<div class="crosslinks">{"".join(cross)}</div>'

    body = f"""
  <p class="kicker">Neighbourhood · #{row['nbhd_code']}</p>
  <h1>{html.escape(nb)}</h1>
  <p class="subhead">{nia_chip}{fmt_int(row['tree_count'])} street trees · {row['area_km2']:.2f} km² · pop. {fmt_int(row['population_2021'])}</p>

  {stats_html}

  <h2>What the numbers say</h2>
  {narr}

  <h2>Most common species here</h2>
  {species_html}

  {big_block}

  <h2>Explore</h2>
  {cross_html}

  <p class="note">
    Tree counts and species from the City of Toronto Street Tree dataset (city-owned trees in the
    road allowance only — not parks or private property). Canopy % and heat proxy derive from the
    2018 land-cover raster. Population is from the 2021 census, joined by the
    158-neighbourhood model.
  </p>
"""
    return head + body + page_foot()


def build_rank_maps(df):
    """For each metric, produce a dict nbhd_code -> 1-indexed rank."""
    metrics = {
        "count_rank": ("tree_count", False),          # higher = better rank 1
        "density_rank": ("trees_per_km2", False),
        "canopy_rank": ("canopy_pct", False),
        "income_rank": ("median_hh_income", False),
        "shannon_rank": ("shannon_h", False),
        "heat_rank": ("heat_proxy", False),
    }
    ranks = {}
    for key, (col, _asc) in metrics.items():
        ordered = df.dropna(subset=[col]).sort_values(col, ascending=False).reset_index(drop=True)
        rank_map = {code: i + 1 for i, code in enumerate(ordered["nbhd_code"])}
        ranks[key] = rank_map
    return ranks


def build_index_page(df):
    title = "Toronto neighbourhoods — street-tree profiles"
    desc = "Tree count, density, canopy, and species diversity for each of Toronto's 158 neighbourhoods."
    head = page_head(title, desc, depth=1)

    # Prepare rows
    rows = df.sort_values("nbhd_name").to_dict(orient="records")
    tbody = []
    for r in rows:
        sl = slug(r["nbhd_name"])
        canopy = f"{r['canopy_pct']:.1f}%" if r.get("canopy_pct") and r["canopy_pct"] == r["canopy_pct"] else "—"
        tbody.append(
            f"<tr>"
            f"<td><a href='{sl}/'>{html.escape(r['nbhd_name'])}</a></td>"
            f"<td class='num' data-v='{int(r['tree_count'])}'>{fmt_int(r['tree_count'])}</td>"
            f"<td class='num' data-v='{int(r['trees_per_km2'])}'>{fmt_int(r['trees_per_km2'])}</td>"
            f"<td class='num' data-v='{r.get('canopy_pct') or 0:.2f}'>{canopy}</td>"
            f"<td class='num' data-v='{r['shannon_h']:.4f}'>{r['shannon_h']:.2f}</td>"
            f"</tr>"
        )

    body = f"""
  <p class="kicker">Neighbourhoods</p>
  <h1>Toronto's 158 neighbourhoods, by the trees</h1>
  <p class="subhead">Every city-owned street tree, by neighbourhood: count, density, canopy coverage, and species diversity.</p>

  <div class="filter"><input id="q" type="text" placeholder="Filter — e.g. Rosedale, Trinity-Bellwoods…" autocomplete="off"></div>

  <table class="ranking" id="tbl">
    <thead>
      <tr>
        <th data-sort="text">Neighbourhood <span class="arrow">↕</span></th>
        <th data-sort="num">Trees <span class="arrow">↕</span></th>
        <th data-sort="num">per km² <span class="arrow">↕</span></th>
        <th data-sort="num">Canopy <span class="arrow">↕</span></th>
        <th data-sort="num">Species H <span class="arrow">↕</span></th>
      </tr>
    </thead>
    <tbody>
      {''.join(tbody)}
    </tbody>
  </table>

  <p class="note">
    H = Shannon diversity index (higher = more varied species mix; 3.5 is good, 4.2+ is excellent).
    Canopy % is 2018 tree cover of the whole neighbourhood (all trees, including parks and private).
    Click a column heading to sort.
  </p>

<script>
(function() {{
  const tbl = document.getElementById('tbl');
  const ths = tbl.querySelectorAll('thead th');
  let sortDir = {{}};
  ths.forEach((th, i) => {{
    th.addEventListener('click', () => {{
      const isNum = th.getAttribute('data-sort') === 'num';
      const rows = Array.from(tbl.querySelectorAll('tbody tr'));
      const dir = sortDir[i] = !sortDir[i];
      rows.sort((a, b) => {{
        const av = isNum ? parseFloat(a.cells[i].dataset.v) : a.cells[i].textContent.trim().toLowerCase();
        const bv = isNum ? parseFloat(b.cells[i].dataset.v) : b.cells[i].textContent.trim().toLowerCase();
        if (av < bv) return dir ? -1 : 1;
        if (av > bv) return dir ? 1 : -1;
        return 0;
      }});
      const tb = tbl.querySelector('tbody');
      rows.forEach(r => tb.appendChild(r));
    }});
  }});
  // Filter
  const q = document.getElementById('q');
  q.addEventListener('input', () => {{
    const v = q.value.toLowerCase().trim();
    tbl.querySelectorAll('tbody tr').forEach(r => {{
      r.style.display = v === '' || r.cells[0].textContent.toLowerCase().includes(v) ? '' : 'none';
    }});
  }});
}})();
</script>
"""
    return head + body + page_foot()


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    merged, top, biggest = load_data()

    ranks = build_rank_maps(merged)
    total = len(merged)

    # Index
    (OUT / "index.html").write_text(build_index_page(merged))
    print(f"wrote {OUT / 'index.html'}")

    # Per-nbhd
    for _, row in merged.iterrows():
        sl = slug(row["nbhd_name"])
        dir_ = OUT / sl
        dir_.mkdir(exist_ok=True)
        code = row["nbhd_code"]

        sp_rows = top[top["nbhd_code"] == code]
        big_rows = biggest[biggest["nbhd_code"] == code]
        big_row = big_rows.iloc[0] if len(big_rows) else None

        row_ranks = {
            "total": total,
            "count_rank": ranks["count_rank"].get(code, total),
            "density_rank": ranks["density_rank"].get(code, total),
            "canopy_rank": ranks["canopy_rank"].get(code, total),
            "income_rank": ranks["income_rank"].get(code, total),
            "shannon_rank": ranks["shannon_rank"].get(code, total),
            "heat_rank": ranks["heat_rank"].get(code, total),
        }

        (dir_ / "index.html").write_text(nbhd_page_html(row.to_dict(), sp_rows, big_row, row_ranks))

    print(f"wrote {total} per-neighbourhood pages under {OUT}")


if __name__ == "__main__":
    main()
