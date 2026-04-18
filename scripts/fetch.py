"""Fetch Toronto Open Data CKAN resources needed for torontotrees."""
from pathlib import Path
import requests

RAW = Path(__file__).resolve().parent.parent / "data" / "raw"

RESOURCES = {
    "street-tree-data-4326.csv": "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/6ac4569e-fd37-4cbc-ac63-db3624c5f6a2/resource/b65cd31d-fabc-4222-83ef-8ddd11295d2b/download/street-tree-data-4326.csv",
    "neighbourhoods-4326.geojson": "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/fc443770-ef0a-4025-9c2c-2cb558bfab00/resource/0719053b-28b7-48ea-b863-068823a93aaa/download/neighbourhoods-4326.geojson",
    "nbhd_2021_census_profile_full_158model.xlsx": "https://ckan0.cf.opendata.inter.prod-toronto.ca/dataset/6e19a90f-971c-46b3-852c-0c48c436d1fc/resource/19d4a806-7385-4889-acf2-256f1e079060/download/nbhd_2021_census_profile_full_158model.xlsx",
}


def fetch(name: str, url: str) -> None:
    out = RAW / name
    if out.exists():
        print(f"skip {name} ({out.stat().st_size / 1e6:.1f} MB)")
        return
    print(f"download -> {name}")
    with requests.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        with out.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    print(f"  done: {out.stat().st_size / 1e6:.1f} MB")


def main() -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    for name, url in RESOURCES.items():
        fetch(name, url)


if __name__ == "__main__":
    main()
