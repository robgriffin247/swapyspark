# SWAPI Data pipeline with PySpark, Parquet and Delta Tables

A quick look at creating a data pipeline with PySpark, Parquet files (saved locally rather than in S3 storage) and Delta tables. Data come from the SWAPI API as a quick (I already have familiarity with data and code to extract as json files) and lightweight data source (a few small tables with relations to build on). PySpark is overkill here but it's just for learning purposes.

#### Prerequisites

- Java (``sudo apt update && sudo apt install default-jdk``) for PySpark
- ``uv`` package/dependency manager
- ``black`` python formatter (``uv run black .`` before commits)

#### Structure

- ``./extract_swapi.py`` contains script to extract SWAPI data and save as *.parquet* files
- ``./data/raw/`` contains the raw *.parquet* files after extraction from SWAPI
- ``./spark_session.py`` defines a function to get the PySpark session for the SWAPI pipeline
- ``./transform_swapi.py`` reads the raw *.parquet* files, performs some transforamtions (broken into a few different assets), and generates the film stats table (number of characeters and average character mass and height per film)
- ``./data/delta/`` contains the delta tables from transformation

#### Running

- Extract data from *.parquet* files
    ```
    uv run extract_swapi.py
    ```
- Transform the data to create the output of film stats 
    ```
    uv run transform_swapi.py
    ```
