"""Replace the straight-line ROUTE polylines in each walk with real
walking geometry from the public OSRM server.

For each walk, we read the stop coordinates, call OSRM to generate a
pedestrian route that visits them in order, and rewrite the
`const ROUTE = [...]` line in the walk's HTML with dense lat/lon
points that follow actual sidewalks and streets.

Idempotent — can be re-run. The OSRM server is a free public
instance at router.project-osrm.org; be polite.
"""
from pathlib import Path
import json
import re
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parent.parent
WALKS = ROOT / "site" / "walks"

OSRM = "https://router.project-osrm.org/route/v1/foot/{coords}?overview=full&geometries=geojson"


def extract_stops(html: str) -> list[tuple[float, float]]:
    """Pull lat/lon pairs from the STOPS array in a walk's HTML."""
    m = re.search(r"const STOPS = \[(.*?)\];", html, re.DOTALL)
    if not m:
        raise RuntimeError("no STOPS array found")
    block = m.group(1)
    pairs = re.findall(r"lat:\s*([-\d.]+)\s*,\s*lon:\s*([-\d.]+)", block)
    return [(float(lat), float(lon)) for lat, lon in pairs]


def osrm_route(waypoints: list[tuple[float, float]]) -> list[list[float]]:
    """Return a dense [[lat, lon], ...] polyline along actual walking paths."""
    coord_str = ";".join(f"{lon},{lat}" for lat, lon in waypoints)
    url = OSRM.format(coords=coord_str)
    r = urllib.request.urlopen(url, timeout=30)
    data = json.load(r)
    if data.get("code") != "Ok":
        raise RuntimeError(f"OSRM: {data.get('code')} — {data.get('message','')}")
    coords = data["routes"][0]["geometry"]["coordinates"]  # [lon, lat]
    return [[round(lat, 6), round(lon, 6)] for lon, lat in coords]


def format_route_js(route: list[list[float]]) -> str:
    return "const ROUTE = " + json.dumps(route, separators=(",", ":")) + ";"


def update_walk(path: Path) -> None:
    html = path.read_text()
    stops = extract_stops(html)
    route = osrm_route(stops)
    new_js = format_route_js(route)
    new_html = re.sub(
        r"const ROUTE = \[[\s\S]*?\];",
        new_js,
        html,
        count=1,
    )
    if new_html == html:
        raise RuntimeError("no ROUTE substitution made")
    path.write_text(new_html)
    print(f"  {path.relative_to(ROOT)}: {len(stops)} stops -> {len(route)} polyline points")


def main() -> None:
    for d in sorted(WALKS.iterdir()):
        if not d.is_dir():
            continue
        html = d / "index.html"
        if not html.exists():
            continue
        try:
            print(f"{d.name}:")
            update_walk(html)
            time.sleep(1)  # be polite to the public OSRM server
        except Exception as e:
            print(f"  SKIP — {e}")


if __name__ == "__main__":
    main()
