from spark_session import get_spark_session
import pyspark.sql.functions as sf
from pathlib import Path


def build_characters(spark_session):

    characters = spark_session.read.parquet("data/raw/people.parquet")

    # Fix numeric type in height and mass
    characters = characters.withColumn("height", sf.regexp_replace("height", ",", ""))
    characters = characters.withColumn("mass", sf.regexp_replace("mass", ",", ""))

    # > Columns to retain from characters
    characters = characters.select(
        sf.col("url").alias("character_url"),
        sf.col("name").alias("character"),
        sf.col("height").try_cast("integer"),
        sf.col("mass").try_cast("float"),
    )

    Path("data/delta").mkdir(parents=True, exist_ok=True)
    characters.write.format("delta").mode("overwrite").save("data/delta/characters")
    return characters


def build_film_characters(spark_session):

    films = spark_session.read.parquet("data/raw/films.parquet")
    characters = spark_session.read.format("delta").load("data/delta/characters")

    # Reshape data
    # > Expand films character array into one row per character per film
    film_characters = films.select(
        sf.col("url").alias("film_url"),
        sf.col("episode_id").alias("episode"),
        sf.col("title").alias("film"),
        sf.explode("characters").alias("character_url"),
    )

    # > Join
    film_characters = film_characters.join(characters, "character_url")

    Path("data/delta").mkdir(parents=True, exist_ok=True)
    film_characters.write.format("delta").mode("overwrite").save(
        "data/delta/film_characters"
    )
    return film_characters


def build_film_stats(spark_session):

    film_characters = spark_session.read.format("delta").load(
        "data/delta/film_characters"
    )

    # Aggregate to films stats; number of characters and average height/mass per film
    film_stats = (
        film_characters.groupBy(["film_url", "film", "episode"])
        .agg(
            sf.count("character_url").alias("character_count"),
            sf.round(sf.avg("mass"), 1).alias("average_mass"),
            sf.round(sf.avg("height"), 1).alias("average_height"),
        )
        .sort("film_url")
        .drop("film_url")
    )

    Path("data/delta").mkdir(parents=True, exist_ok=True)
    film_stats.write.format("delta").mode("overwrite").save("data/delta/film_stats")
    return film_stats


if __name__ == "__main__":

    spark_session = get_spark_session()

    build_characters(spark_session)
    build_film_characters(spark_session)
    build_film_stats(spark_session)

    # Verify
    spark_session.read.format("delta").load("data/delta/characters").show()
    spark_session.read.format("delta").load("data/delta/film_characters").show()
    spark_session.read.format("delta").load("data/delta/film_stats").show()
