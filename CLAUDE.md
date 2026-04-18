# torontotrees — street trees + canopy equity

## Who you're working with

Tyler is a Toronto resident and civic-minded builder. He previously shipped **swimcal.ca** — subscribable ICS calendar feeds for Toronto public pool swim schedules, built on top of City of Toronto open data (open.toronto.ca). He loves Toronto's "city in a forest" reputation and wants to build something interactive around the street-tree data.

## What this project is

Two sides of the same project, sharing infrastructure:

1. **Product side — torontotrees** (charm project): interactive map/site. "What species is the tree in front of my house?" Rare-species finder. Possibly bloom-time hints, size/age approximations from DBH (diameter at breast height), neighbourhood species variety.
2. **Analytics side — tree canopy equity**: cross-walk the street-tree inventory with neighbourhood demographics (income, visible minority population, tenure). Which wards/neighbourhoods are tree-poor? What's the species-diversity gap? How has Emerald Ash Borer attrition reshaped the canopy over the last decade? Are new plantings closing equity gaps or widening them?

This project is one of three parallel Toronto open-data threads. The others (separate directories, separate Claude sessions) are `bikeshare-od-flows/` and `mycouncillor/`. Do not bleed scope across them.

## Datasets to start from

All on open.toronto.ca. Verify slugs when fetching.

- **Street Tree Data** — hundreds of thousands of records, one row per tree, with species (common and botanical), DBH, lat/lon, ward, and often planting date. This is the spine of both the product and the analytics.
- **Neighbourhood Boundaries** / **Ward Boundaries** — for spatial joins and aggregations.
- **Neighbourhood Profiles** (census-based) — for the demographic layer of the equity analysis. Watch out for census-year cadence (data typically lags 5 years).
- **Tree Planting data** — if published separately, useful for the "is the city closing the equity gap?" question.
- **Emerald Ash Borer** related records — the city published ash-tree removal plans; historical snapshots of the street-tree inventory (if available) would let you quantify canopy loss over time.

## Hazards / things to verify early

- The street tree dataset covers **city-owned street trees** (boulevard, road allowance). It does **not** include private-property trees or park trees (often separate datasets). Be explicit about this in any public framing — "canopy" in this data is not total canopy.
- For true canopy analysis, LiDAR-based canopy cover layers exist (city + UFORE studies). Worth exploring for a richer equity story, but adds scope.
- Species names are not always normalized — the same species can appear under multiple common-name spellings. Early cleaning task.
- Historical comparisons: if you want to see change over time, you need old snapshots. The current dataset is a single point-in-time view. Confirm whether old snapshots are archived anywhere.

## Analytical / design questions worth pulling on

- Canopy density per neighbourhood × median household income scatterplot — is there a correlation? (Very likely yes, based on parallel studies in other North American cities — the *size* and *shape* are the interesting part.)
- Species diversity (Shannon index or similar) per neighbourhood — monocultures are climate/pest risks.
- Age/size structure by neighbourhood — young plantings imply investment; old canopy implies inheritance.
- Species hit-list by ward — which wards lost the most trees to EAB? Have they been replanted?
- For the product: what's the most delightful "tree near me" experience? A list? A map? A "tree of the day"?

## Preferred shape for output

Tyler likes interactive maps and analytics that reveal something non-obvious. He wants the product side to feel delightful (he said "charm project"), and the analytics side to be blog-post-quality findings. He does *not* want to lead with a generic ICS calendar pattern — that's solved.

## Tooling

Not yet chosen. EDA/analytics is a good fit for Python + uv + Jupyter + GeoPandas + DuckDB. The interactive map frontend is an open question — MapLibre/Mapbox + a static site, or deck.gl / kepler.gl. Confirm with Tyler before standing up scaffolding.

## Working-style notes

- Genuinely useful > technical demo.
- Honest about data gaps; don't paper over limitations.
- Ad-hoc notebooks are valid intermediate artefacts.

## Memory

Session memory accumulates at `~/.claude/projects/-Users-tyler-src-open-data-toronto-torontotrees/memory/`. Parent context: `~/.claude/projects/-Users-tyler-src-open-data-toronto/memory/`.
