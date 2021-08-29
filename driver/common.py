import os
from pyspark.sql import SparkSession, DataFrame


def read_csv(path: str) -> DataFrame:
    return (
        SPARK.read
        .format("csv")
        .option("mode", "DROPMALFORMED")
        .option("header", "true")
        .load(path))


def write_csv(df: DataFrame, output_path: str, buckets=3) -> None:
    df.coalesce(buckets).write.format("csv").mode("overwrite").options(header="true").save(
        path=output_path)
