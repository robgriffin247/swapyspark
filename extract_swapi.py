import httpx
import polars as pl
import duckdb


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
    for e in ["people", "films", "planets", "vehicles", "starships", "species"]:
        data = extract_swapi(endpoint=e, verbose=True)
        with open(f"data/raw/{e}.parquet", "w") as f:
            pl.DataFrame(data).write_parquet(f)

    # Verify
    with duckdb.connect() as con:
        films = con.sql("select * from 'data/raw/films.parquet'").pl()
        print(films)
