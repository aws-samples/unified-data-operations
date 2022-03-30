# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from driver import driver
from driver.task_executor import DataSet


def find_dataset_by_id(dss: List[DataSet], dataset_id):
    return next(iter([ds for ds in dss if ds.id == dataset_id]), None)


def remap_schema(ds: DataFrame) -> List[StructType]:
    schema_fields = list()
    for col in ds.model.columns:
        if hasattr(col, 'transform') and 'skip' in [t.type for t in col.transform]:
            continue
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
