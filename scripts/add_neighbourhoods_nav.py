"""One-shot idempotent patch: insert a 'Neighbourhoods' link into the existing
site nav on every page that already has the Walks-then-About pattern.

Writes nothing if Neighbourhoods is already present.
"""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parent.parent / "site"

# Matches: <a href="[prefix]walks/">Walks</a><a href="[prefix]about/">About</a>
# where [prefix] can vary (../, ../../, etc.)
PATTERN = re.compile(
    r'(<a href="(?P<prefix>[^"]*)walks/">Walks</a>)(\s*)(<a href="[^"]*about/">About</a>)'
)


MAINNAV_BLOCK = re.compile(r'<nav class="mainnav">.*?</nav>', re.DOTALL)


def patch(p: Path) -> bool:
    s = p.read_text()
    nav_match = MAINNAV_BLOCK.search(s)
    if nav_match and "Neighbourhoods</a>" in nav_match.group(0):
        return False
    m = PATTERN.search(s)
    if not m:
        return False
    insert = f'<a href="{m.group("prefix")}neighbourhoods/">Neighbourhoods</a>'
    new_s = PATTERN.sub(rf'\1\3{insert}\3\4', s, count=1)
    if new_s == s:
        return False
    p.write_text(new_s)
    return True


def main():
    count = 0
    for p in ROOT.rglob("*.html"):
        if patch(p):
            count += 1
            print(f"  patched {p.relative_to(ROOT)}")
    print(f"done — patched {count} files")


if __name__ == "__main__":
    main()
