"""Generate subscribable .ics calendar feeds from site/data/calendar.json.

Each feed is a curated bundle of species, with year-recurring all-day events
for each species' bloom (or fall-colour) window. Imports/subscribes cleanly
in Apple Calendar, Google Calendar, Outlook, Fantastical, etc.

Output:
  site/feeds/all-blooms.ics             — every bloom window
  site/feeds/cherries.ics               — Prunus + magnolia + redbud spring bloomers
  site/feeds/fragrant.ics               — fragrant_bloom tagged
  site/feeds/showy-bloom.ics            — showy_bloom tagged
  site/feeds/fall-colour.ics            — every showy_fall window
  site/feeds/all-events.ics             — bloom + fall, everything

The events use FREQ=YEARLY so a one-time subscribe gives you reminders every
year going forward. Bloom windows are a few weeks ± weather; the City data
+ Toronto-typical phenology gives a usable midpoint.
"""
from pathlib import Path
import datetime as dt
import json
import hashlib

ROOT = Path(__file__).resolve().parent.parent
CAL = ROOT / "site" / "data" / "calendar.json"
OUT = ROOT / "site" / "feeds"
OUT.mkdir(parents=True, exist_ok=True)

CAL_PRODID = "-//treeto.ca//Tree Bloom Calendar//EN"
SITE = "https://treeto.ca"


def fmt_date(year: int, m: int, d: int) -> str:
    """Date in basic YYYYMMDD format for DTSTART;VALUE=DATE."""
    return f"{year:04d}{m:02d}{d:02d}"


def add_days(year: int, m: int, d: int, days: int) -> tuple[int, int, int]:
    new = dt.date(year, m, d) + dt.timedelta(days=days)
    return new.year, new.month, new.day


def slug(s: str) -> str:
    return "".join(c if c.isalnum() else "-" for c in s.lower()).strip("-")


def build_event(species: dict, kind: str, year: int) -> str:
    """One VEVENT for a species' bloom or fall window. RRULE makes it yearly."""
    period = species.get(kind)
    if not period:
        return ""
    s = period["start"]
    e = period["end"]
    # iCalendar DTEND is exclusive for VALUE=DATE — add 1 day.
    ey, em, ed = add_days(year, e["m"], e["d"], 1)
    common = species["common"].split(",")
    if len(common) >= 2 and common[0].split()[0].isalpha():
        # "Maple, Norway" → "Norway Maple"
        title = f"{common[1].strip()} {common[0].strip()}".title()
    else:
        title = species["common"]
    if kind == "bloom":
        summary = f"🌸 {title} in bloom (Toronto)"
    else:
        summary = f"🍁 {title} fall colour (Toronto)"
    note = period.get("note", "")
    desc = f"{title} — {note}\\n\\n{species.get('botanical', '')}\\n\\nFrom treeto.ca · estimated window for typical Toronto phenology."
    uid = hashlib.sha1(f"{species['key']}|{kind}".encode()).hexdigest()[:16]
    lines = [
        "BEGIN:VEVENT",
        f"UID:{uid}@treeto.ca",
        f"DTSTAMP:{dt.datetime.now(dt.UTC).strftime('%Y%m%dT%H%M%SZ')}",
        f"DTSTART;VALUE=DATE:{fmt_date(year, s['m'], s['d'])}",
        f"DTEND;VALUE=DATE:{fmt_date(ey, em, ed)}",
        "RRULE:FREQ=YEARLY",
        f"SUMMARY:{summary}",
        f"DESCRIPTION:{desc}",
        f"URL:{SITE}/calendar/",
        "TRANSP:TRANSPARENT",
        "END:VEVENT",
    ]
    return "\r\n".join(lines)


def build_feed(name: str, description: str, species_list: list[dict], kinds: list[str]) -> str:
    """Wrap a list of events in a VCALENDAR header."""
    year = dt.date.today().year
    body = []
    for sp in species_list:
        for kind in kinds:
            ev = build_event(sp, kind, year)
            if ev:
                body.append(ev)
    cal = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        f"PRODID:{CAL_PRODID}",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        f"X-WR-CALNAME:{name}",
        f"X-WR-CALDESC:{description}",
        "X-WR-TIMEZONE:America/Toronto",
        *body,
        "END:VCALENDAR",
    ]
    # iCalendar wants CRLF line endings.
    return "\r\n".join(cal) + "\r\n"


def main() -> None:
    cal = json.loads(CAL.read_text())

    # Helper: bloomers vs fall trees, with tag filters
    def has_tag(sp, t):
        return t in (sp.get("tags") or [])

    bloomers = [s for s in cal if s.get("bloom")]
    fall_showy = [s for s in cal if has_tag(s, "showy_fall")]
    fragrant = [s for s in cal if has_tag(s, "fragrant_bloom")]
    showy_blooms = [s for s in cal if has_tag(s, "showy_bloom")]
    # Spring cherries / cherry-blossom-like spring bloomers
    cherries = [s for s in cal if (s.get("key", "").startswith("prunus")
                                    or s["key"] == "magnolia soulngeana"
                                    or s["key"] == "cercis canadensis"
                                    or s["key"] == "amelanchier canadensis"
                                    or s["key"] == "malus sargentii")
                and s.get("bloom")]

    feeds = [
        ("all-blooms.ics",   "Toronto tree blooms",         "Every species' estimated bloom window in Toronto",         bloomers,    ["bloom"]),
        ("cherries.ics",     "Toronto cherry blossoms",     "Cherries, magnolia, redbud, serviceberry, crabapple",       cherries,    ["bloom"]),
        ("fragrant.ics",     "Toronto fragrant trees",      "Linden, basswood, black locust, yellowwood, lilac",         fragrant,    ["bloom"]),
        ("showy-bloom.ics",  "Toronto showy blooms",        "Magnolia, redbud, catalpa, horsechestnut, etc.",            showy_blooms,["bloom"]),
        ("fall-colour.ics",  "Toronto fall colour",         "Sugar maple, red maple, ginkgo, oak — peak weeks",          fall_showy,  ["fall"]),
        ("all-events.ics",   "Toronto trees — all events",  "Every bloom and fall window on treeto.ca",                  cal,         ["bloom", "fall"]),
    ]

    summary = []
    for filename, name, desc, species_list, kinds in feeds:
        text = build_feed(name, desc, species_list, kinds)
        path = OUT / filename
        path.write_text(text)
        # Count events
        n = text.count("BEGIN:VEVENT")
        summary.append((filename, name, n, len(text)))
        print(f"  {filename}: {n} events, {len(text):,} bytes")
    print(f"wrote {len(feeds)} feeds to {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
