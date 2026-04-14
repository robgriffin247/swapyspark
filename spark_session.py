from pyspark.sql import SparkSession


def get_spark_session(app_name="swapi_pipeline"):
    session = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "io.delta:delta-spark_4.1_2.13:4.1.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("WARN")

    return session


if __name__ == "__main__":
    spark_session = get_spark_session()
    print(f"Spark version: {spark_session.version}")
