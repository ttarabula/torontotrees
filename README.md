# torontotrees

Two things sharing one data pipeline:

- **Product** — a static site that tells you what species of tree is in front of any Toronto address. Live locally at `site/index.html`.
- **Analytics** — canopy-equity findings: tree density and species diversity across Toronto's 158 neighbourhoods, cross-walked with 2021 census income data.

Built on the [City of Toronto Street Tree Data](https://open.toronto.ca/dataset/street-tree-data/) (689,013 trees on city road allowances).

## Setup

Uses [uv](https://github.com/astral-sh/uv) for Python and venv management.

```
uv sync
```

## Build pipeline

Four scripts, run in order:

```
uv run scripts/fetch.py          # download raw CSV + GeoJSON + XLSX to data/raw/ (~149 MB)
uv run scripts/build.py          # normalize, spatial-join, emit data/processed/*.parquet
uv run scripts/fetch_species.py  # 1-time: Wikipedia summary per species -> site/data/species.json
uv run scripts/export_site.py    # per-street JSONs -> site/data/street/*.json
```

For analytics only:

```
uv run scripts/analyze.py        # prints the findings tables
uv run scripts/charts.py         # writes charts/*.png
```

## Serve locally

```
cd site && uv run python -m http.server 8000
```

Open <http://localhost:8000>.

## Deploy

The `site/` directory is fully static (~111 MB, ~9,100 files; compresses well). Any static host works:

**Cloudflare Pages** (recommended — generous file count limit):

```
npx wrangler pages deploy site/ --project-name=torontotrees
```

**Netlify**:

```
npx netlify deploy --dir=site --prod
```

**GitHub Pages**: push `site/` to a `gh-pages` branch (site fits within the 1 GB limit).

Before deploying, confirm `site/data/street/` has all 9,100 JSON files — rerun `scripts/export_site.py` if not.

## Data sources

- [Street Tree Data](https://open.toronto.ca/dataset/street-tree-data/) — City of Toronto, OGL-Toronto
- [Neighbourhoods (158-model)](https://open.toronto.ca/dataset/neighbourhoods/) — City of Toronto
- [Neighbourhood Profiles, 2021 census](https://open.toronto.ca/dataset/neighbourhood-profiles/) — City of Toronto / Statistics Canada
- [Species planted on streets](https://www.toronto.ca/services-payments/water-environment/trees/tree-planting/species-planted-on-streets/) — City of Toronto
- Per-species summaries and thumbnails — Wikipedia via the REST summary API
