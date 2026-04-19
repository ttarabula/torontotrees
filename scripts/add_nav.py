"""Patch the <header class="site"> block in every child page to include a
visible home link (with arrow prefix) and a small nav bar.

Idempotent: detects if the patch has already been applied (nav.mainnav
present) and skips.

Leaves the homepage (/site/index.html) untouched — it has its own layout.
"""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent.parent / "site"
HOME = ROOT / "index.html"


def relative_prefix(file_path: Path) -> str:
    """Return the relative prefix to reach the site root from file_path.

    /site/about/index.html               → ../
    /site/blog/canopy-equity/index.html  → ../../
    """
    rel = file_path.relative_to(ROOT).parts[:-1]   # drop index.html
    return "../" * len(rel) or "./"


NAV_CSS = """
  /* Site nav — added by scripts/add_nav.py */
  header.site .inner { display: flex; justify-content: space-between; align-items: center; gap: 1rem 1.5rem; flex-wrap: wrap; }
  header.site a.home { text-decoration: none; }
  header.site a.home::before { content: "← "; opacity: 0.55; }
  header.site a.home:hover { text-decoration: underline; }
  header.site nav.mainnav { display: flex; gap: 1.1rem; font-size: .9rem; flex-wrap: wrap; }
  header.site nav.mainnav a { color: #666; text-decoration: none; }
  header.site nav.mainnav a:hover { color: #1e5b2d; text-decoration: underline; }
  @media (max-width: 520px) {
    header.site nav.mainnav { font-size: .82rem; gap: .8rem; }
  }
"""


def patch_file(p: Path) -> bool:
    s = p.read_text()
    if "nav.mainnav" in s or "nav class=\"mainnav\"" in s:
        return False  # already patched
    if "header.site" not in s:
        return False  # no site header
    if "<span class=\"tag\">" not in s:
        return False  # different template, skip

    prefix = relative_prefix(p)
    nav_html = (
        f'<nav class="mainnav">'
        f'<a href="{prefix}blog/">Blog</a>'
        f'<a href="{prefix}calendar/">Calendar</a>'
        f'<a href="{prefix}walks/">Walks</a>'
        f'<a href="{prefix}about/">About</a>'
        f'</nav>'
    )

    # Replace the tag span with the nav.
    new = re.sub(
        r'<span class="tag">[^<]*</span>',
        nav_html,
        s,
        count=1,
    )
    if new == s:
        return False

    # Inject CSS right before the closing </style> tag on the first occurrence
    new = re.sub(r"</style>", NAV_CSS + "\n</style>", new, count=1)
    p.write_text(new)
    return True


def main():
    patched = 0
    skipped = 0
    for p in sorted(ROOT.rglob("*.html")):
        if p == HOME:
            continue
        if patch_file(p):
            patched += 1
            print(f"  patched {p.relative_to(ROOT)}")
        else:
            skipped += 1
    print(f"\n{patched} patched, {skipped} skipped")


if __name__ == "__main__":
    main()
