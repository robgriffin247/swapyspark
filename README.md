# SWAPI Data pipeline with PySpark, Parquet and Delta Tables

A quick look at creating a data pipeline with PySpark, Parquet files (saved locally rather than in S3 storage) and Delta tables. Data come from the SWAPI API as a quick (I already have familiarity with data and code to extract as json files) and lightweight data source (a few small tables with relations to build on). PySpark is overkill here but it's just for learning purposes.

```
SWAPI API → raw parquet → delta/characters → delta/film_characters → delta/film_stats
```

The project builds this film statistics table

|                film|episode|character_count|average_mass|average_height|
|--------------------|-------|---------------|------------|--------------|
|          A New Hope|      4|             18|       155.7|         170.3|
|The Empire Strike...|      5|             16|        81.0|         169.3|
|  Return of the Jedi|      6|             20|       146.7|         161.9|
|  The Phantom Menace|      1|             34|       120.6|         169.7|
|Attack of the Clones|      2|             40|        70.7|         175.0|
| Revenge of the Sith|      3|             34|        78.5|         177.8|


#### Prerequisites

- Java (``sudo apt update && sudo apt install default-jdk``) for PySpark
- ``uv`` package/dependency manager
- ``black`` python formatter (``uv run black .`` before commits)


#### Structure

- [``./extract_swapi.py``](extract_swapi.py) contains script to extract SWAPI data and save as *.parquet* files
- ``./data/raw/`` contains the raw *.parquet* files after extraction from SWAPI
- [``./spark_session.py``](spark_session.py) defines a function to get the PySpark session for the SWAPI pipeline
- [``./transform_swapi.py``](transform_swapi.py) reads the raw *.parquet* files, performs some transformations (broken into a few different assets), and generates the film stats table (number of characters and average character mass and height per film)
- ``./data/delta/``  contains the delta tables from transformation


#### Running

- Extract data from *.parquet* files
    ```
    uv run extract_swapi.py
    ```
- Transform the data to create the output of film stats 
    ```
    uv run transform_swapi.py
    ```
