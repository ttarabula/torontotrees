"""Generate site/blog/rss.xml from the per-post HTML files.

Scans every site/blog/<slug>/index.html and extracts:
  - <title> (strips " | torontotrees")
  - <meta name="description">
  - first line of the .subhead div (for pub date, expects "Month YYYY ·")
  - first <h1>

Run from repo root:  uv run scripts/rss.py
"""
from pathlib import Path
import datetime as dt
import html
import re

ROOT = Path(__file__).resolve().parent.parent
BLOG = ROOT / "site" / "blog"
BASE = "https://ttarabula.github.io/torontotrees"

MONTHS = {m: i for i, m in enumerate(
    ["January","February","March","April","May","June",
     "July","August","September","October","November","December"], start=1)}


def parse_post(index_html: Path):
    slug = index_html.parent.name
    html_text = index_html.read_text()
    title_m = re.search(r"<title>(.*?)</title>", html_text, re.DOTALL)
    desc_m = re.search(r'<meta[^>]*name=["\']description["\'][^>]*content=["\'](.*?)["\']', html_text)
    subhead_m = re.search(r'<div class="subhead">([^<]+)', html_text)

    title = title_m.group(1).strip() if title_m else slug
    title = re.sub(r"\s*\|\s*torontotrees\s*$", "", title)
    description = html.unescape(desc_m.group(1).strip()) if desc_m else ""

    pub_date = None
    if subhead_m:
        s = subhead_m.group(1).strip()
        m = re.match(r"([A-Za-z]+)\s+(\d{4})", s)
        if m and m.group(1) in MONTHS:
            pub_date = dt.datetime(int(m.group(2)), MONTHS[m.group(1)], 1, 12, 0, 0, tzinfo=dt.timezone.utc)
    if pub_date is None:
        pub_date = dt.datetime.fromtimestamp(index_html.stat().st_mtime, tz=dt.timezone.utc)

    return {
        "slug": slug,
        "title": title,
        "description": description,
        "url": f"{BASE}/blog/{slug}/",
        "pub_date": pub_date,
    }


def rss_date(d: dt.datetime) -> str:
    return d.strftime("%a, %d %b %Y %H:%M:%S %z")


def esc(s: str) -> str:
    return html.escape(s, quote=True)


def main():
    posts = []
    for idx in BLOG.glob("*/index.html"):
        posts.append(parse_post(idx))
    posts.sort(key=lambda p: p["pub_date"], reverse=True)
    now = dt.datetime.now(tz=dt.timezone.utc)

    items = []
    for p in posts:
        items.append(
            f"""    <item>
      <title>{esc(p['title'])}</title>
      <link>{p['url']}</link>
      <guid isPermaLink="true">{p['url']}</guid>
      <pubDate>{rss_date(p['pub_date'])}</pubDate>
      <description>{esc(p['description'])}</description>
    </item>""")

    feed = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>torontotrees</title>
    <link>{BASE}/</link>
    <atom:link href="{BASE}/blog/rss.xml" rel="self" type="application/rss+xml"/>
    <description>Essays and visualizations built on Toronto's open street-tree data.</description>
    <language>en-CA</language>
    <lastBuildDate>{rss_date(now)}</lastBuildDate>
{chr(10).join(items)}
  </channel>
</rss>
"""
    out = BLOG / "rss.xml"
    out.write_text(feed)
    print(f"wrote {out} — {len(posts)} posts")


if __name__ == "__main__":
    main()
