import json
import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DataType, StructType

from driver import driver
from driver.task_executor import DataSet


class ValidationException(Exception):
    pass


def get_data_set(dss: list[DataSet], dataset_id):
    return next(iter([ds for ds in dss if ds.id == dataset_id]), None)


def remap_schema(ds: DataFrame):
    schema_fields = list()
    for col in ds.model.columns:
        nullable = True
        if hasattr(col, 'constraints'):
            nullable = 'not_null' not in [c.type for c in col.constraints]
        schema_fields.append({'metadata': {}, 'name': col.id, 'type': col.type, 'nullable': nullable})
    return StructType.fromJson({'fields': schema_fields, 'type': 'struct'})


def read_csv(path: str) -> DataFrame:
    return (
        driver.get_spark().read
            .format("csv")
            .option("mode", "DROPMALFORMED")
            .option("header", "true")
            .load(path))


def write_csv(df: DataFrame, output_path: str, buckets=3) -> None:
    df.coalesce(buckets).write.format("csv").mode("overwrite").options(header="true").save(
        path=output_path)
