"""Pick the monthly blog-post topic menu.

Invoked by the monthly-blog-prompt GitHub Action. Emits a GitHub Issue
title and body to stdout (the workflow pipes this to `gh issue create`).

Parity rule (Tyler's spec):
  Even calendar month → species spotlight (Feb, Apr, Jun, Aug, Oct, Dec)
  Odd  calendar month → miscellaneous post (Jan, Mar, May, Jul, Sep, Nov)

The script suggests a menu of candidates, excluding species/themes that
already have a post in site/blog/. Final topic choice is the writer's.
"""
from pathlib import Path
import datetime as dt
import sys

ROOT = Path(__file__).resolve().parent.parent
BLOG = ROOT / "site" / "blog"

SPECIES_CANDIDATES = [
    ("Norway maple", "Acer platanoides", "The most-planted tree in Toronto (>99K of them) — and the one the city no longer plants because it's invasive. The paradox of a beloved urban workhorse on the bad-species list."),
    ("Sugar maple", "Acer saccharum", "The Canadian flag species is rare on Toronto's streets. Why the urban environment is brutal for it, and where the remaining big ones are."),
    ("Red oak", "Quercus rubra", "Favoured in the current planting program for its ecological value. Hundreds of native insect species depend on oaks."),
    ("Bur oak", "Quercus macrocarpa", "300+ years old in places. Toronto's savanna-relict tree, predating the city itself."),
    ("American elm", "Ulmus americana", "The species Dutch elm disease nearly erased. 'Valley Forge' and 'Princeton' cultivars are the resistant comeback."),
    ("London plane", "Platanus × acerifolia", "Some of Toronto's biggest street trees. A hybrid that tolerates salt, drought, compacted soil, and gets enormous."),
    ("Kentucky coffeetree", "Gymnocladus dioicus", "Ontario-native, once endangered, now a favourite in new plantings. Distinctive architecture, tough constitution."),
    ("Littleleaf linden", "Tilia cordata", "The reliable workhorse. Fragrant June bloom, bees lose their minds, a thousand-year European civic tradition."),
    ("Hackberry", "Celtis occidentalis", "Toronto's underused native — the city has been planting more of them since the 2010s as a replacement for the engineered cultivars."),
    ("Katsura", "Cercidiphyllum japonicum", "Heart-shaped leaves that smell like caramel when they fall. A prized recent-plantings specimen."),
    ("Silver maple", "Acer saccharinum", "Fast, brittle, root-heaving. A mid-20th-century favourite the city regrets."),
    ("Freeman maple", "Acer × freemanii", "The 2000s engineered hybrid ('Autumn Blaze'). Brilliant red fall colour, planted everywhere."),
    ("Serviceberry", "Amelanchier canadensis", "White April flowers, red June berries, orange-red October. A tiny tree doing four jobs."),
    ("Tulip tree", "Liriodendron tulipifera", "Carolinian species at the edge of its northern range. Tall, straight, the tulip-shaped flowers 20m up in the canopy."),
    ("Eastern redbud", "Cercis canadensis", "Magenta blooms on bare branches in late April. The first showy colour of spring, planted for small lots since the 1990s."),
    ("Horsechestnut", "Aesculus hippocastanum", "European, grand, declining. Conker-flowers, leaf-scorch, why the city plants fewer each year."),
    ("Japanese tree lilac", "Syringa reticulata", "Creamy June panicles, compact enough for residential boulevards — the wire-friendly small tree."),
    ("Black walnut", "Juglans nigra", "Allelopathic, long-lived, self-sovereign. Why almost nothing grows under one."),
]

MISC_CANDIDATES = [
    "**The Norway-maple paradox** — the most-planted tree (99,860 of them, 14% of the whole canopy) is one the city no longer plants. Why the city is effectively waiting for 70,000 of them to age out.",
    "**Street-tree lifespans** — why a 250-year species lives 30 years on a downtown street. Sidewalk compaction, salt, trunk wounds, utility pruning.",
    "**The 4-to-1 ratio** — 689K street trees, 2.8M Torontonians. One tree for every four of us. What that number hides (parks, ravines, private property).",
    "**The saplings vs stumps ledger** — net change in the canopy, year over year. Is the city actually ahead or just running in place?",
    "**The city's spacing rule** — why most boulevard trees are 8 m apart and what happens on the blocks where that rule was ignored (good and bad).",
    "**The 10 worst blocks for trees** — the named, mapped underdogs of the dataset. Why they look that way, and what the city plans to do about it.",
    "**Species-naming chaos** — how 'Maple, Norway' and 'Norway Maple' and 'Acer platanoides' can all be the same tree, and why that matters for analysis.",
    "**Ravine shadows** — every tree the dataset doesn't count. LiDAR vs. inventory — what the canopy looks like when you stop caring about ownership.",
    "**The marcescent pantheon** — trees that hold their leaves brown into winter (beech, some oaks, hornbeam). Where to see them in January.",
    "**Spring watch** — a week-by-week diary of what bloomed first in Toronto this spring, compared to predicted windows.",
    "**The bike-network canopy** — does a protected bike lane come with trees? Spatial join between bike infrastructure and tree density.",
    "**Three trees I can't explain** — data oddities: a palm in North York, a 250 cm 'oak' in Forest Hill, a tree at coordinates (0, 0). The fun of data-quality archaeology.",
    "**The worst and best intersections** — data-driven rankings of Toronto's tree-dense and tree-starved corners.",
    "**What a tree's DBH tells us (and what it doesn't)** — the math of age-from-diameter, species-specific growth rates, and the ±40% error bar.",
    "**Shared-root co-walks** — which streets have street trees that are almost certainly the same age, planted in the same city contract.",
    "**Species we said goodbye to** — ash, ornamental pear, Siberian elm. The trees Toronto loved and then stopped planting.",
]


def existing_species_slugs():
    slugs = set()
    for d in BLOG.iterdir():
        if d.is_dir():
            slugs.add(d.name.replace("_", "-"))
    return slugs


def pick_species(month_index: int) -> tuple[str, str, str]:
    existing = existing_species_slugs()
    # Filter candidates whose common-name slug is already a blog dir
    available = [
        (c, b, h) for c, b, h in SPECIES_CANDIDATES
        if c.lower().replace(" ", "-").replace("'", "") not in existing
    ]
    pool = available or SPECIES_CANDIDATES
    return pool[month_index % len(pool)]


def pick_misc(month_index: int) -> str:
    return MISC_CANDIDATES[month_index % len(MISC_CANDIDATES)]


def month_index(year: int, month: int) -> int:
    return year * 12 + (month - 1)


def render_issue(year: int, month: int) -> tuple[str, str]:
    month_name = dt.date(year, month, 1).strftime("%B")
    is_species = (month % 2 == 0)   # Feb/Apr/Jun/Aug/Oct/Dec
    mi = month_index(year, month)
    title = f"Blog post — {month_name} {year}"

    if is_species:
        common, botanical, hook = pick_species(mi)
        body_type = "Species spotlight"
        rotation_pick = f"This month's rotated suggestion: **{common}** (*{botanical}*) — {hook}"
        alternatives_lines = []
        for c, b, h in SPECIES_CANDIDATES:
            if c == common:
                continue
            alternatives_lines.append(f"- **{c}** — *{b}*: {h}")
        alternatives = "\n".join(alternatives_lines[:6])
        menu = f"### Other species on the shelf\n{alternatives}"
    else:
        rotated = pick_misc(mi)
        body_type = "Miscellaneous post"
        rotation_pick = f"This month's rotated suggestion: {rotated}"
        others = [m for m in MISC_CANDIDATES if m != rotated]
        menu = "### Other themes on the shelf\n" + "\n".join(f"- {m}" for m in others[:6])

    body = f"""## {body_type} — {month_name} {year}

{rotation_pick}

> **Variety reminder.** Some posts should be deeply scientific / technical (dendrochronology, species ecology, data methodology); others should be storytelling-driven (history, place, characters). Please aim for the mode that's been *missing* lately — if the last two posts were both data-heavy, make this one a story, and vice versa.

{menu}

### Writing notes

- Length: 500–1,500 words is the sweet spot on this site.
- Keep the "city-owned street trees only" caveat where relevant.
- Link forward/backward to related posts (existing post list: `/blog/`).
- Add the post to `site/blog/` as a new directory with `index.html`.
- Update `site/blog/index.html`'s post grid, and regenerate `site/blog/rss.xml` if the post feed script exists.

Close this issue when the post ships.
"""
    return title, body


def main():
    today = dt.date.today()
    year = int(sys.argv[1]) if len(sys.argv) > 1 else today.year
    month = int(sys.argv[2]) if len(sys.argv) > 2 else today.month
    title, body = render_issue(year, month)
    # Emit title on first line, then two blank lines, then body
    print(title)
    print()
    print(body)


if __name__ == "__main__":
    main()
