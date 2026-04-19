"""Generate site/sitemap.xml from every index.html in site/.

Run at deploy time (after all per-page generators). Lists every URL on
treeto.ca with its <lastmod> drawn from file mtime.
"""
from pathlib import Path
import datetime as dt

ROOT = Path(__file__).resolve().parent.parent
SITE = ROOT / "site"
BASE = "https://treeto.ca"
OUT = SITE / "sitemap.xml"


def url_for(path: Path) -> str:
    rel = path.parent.relative_to(SITE).as_posix()
    if rel == ".":
        return f"{BASE}/"
    return f"{BASE}/{rel}/"


def main() -> None:
    pages = sorted(SITE.rglob("index.html"))
    urls = []
    for p in pages:
        loc = url_for(p)
        lastmod = dt.date.fromtimestamp(p.stat().st_mtime).isoformat()
        urls.append(f"  <url><loc>{loc}</loc><lastmod>{lastmod}</lastmod></url>")

    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(urls)
        + "\n</urlset>\n"
    )
    OUT.write_text(xml)
    print(f"wrote {OUT.relative_to(ROOT)} — {len(urls)} URLs")


if __name__ == "__main__":
    main()
