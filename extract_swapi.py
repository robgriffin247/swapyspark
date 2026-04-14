import httpx
import polars as pl
from pathlib import Path


def extract_swapi(endpoint="planets", verbose=False):
    next_page = f"https://swapi.dev/api/{endpoint}"
    data = []

    while next_page:
        if verbose:
            print(f"Getting {next_page}")

        response = httpx.get(url=next_page)
        response.raise_for_status()

        content = response.json()
        data += content["results"]
        next_page = content["next"]

    if verbose:
        print(f"{len(data)} records returned!")

    return data


if __name__ == "__main__":
    for e in ["people", "films"]:
        data = extract_swapi(endpoint=e, verbose=True)
        Path("data/raw").mkdir(parents=True, exist_ok=True)
        pl.DataFrame(data).write_parquet(f"data/raw/{e}.parquet")

    # Verify
    print(pl.read_parquet("data/raw/films.parquet"))
