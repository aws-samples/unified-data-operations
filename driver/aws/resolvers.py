from typing import List
from mypy_boto3_glue.type_defs import TableTypeDef, StorageDescriptorTypeDef, ColumnTypeDef
from pyspark.sql import DataFrame
from driver.task_executor import DataSet


def resolve_table(data_set: DataSet) -> TableTypeDef:
    return TableTypeDef(
        Name=data_set.model_id,
        StorageDescriptor=resolve_storage_descriptor(data_set)
    )


def resolve_storage_descriptor(data_set: DataSet) -> StorageDescriptorTypeDef:
    options = data_set.model.storage.options

    return StorageDescriptorTypeDef(
        Location=options.location,
        Columns=resolve_columns(data_set.df)
    )


def resolve_columns(data_frame: DataFrame) -> List[ColumnTypeDef]:
    columns: List[ColumnTypeDef] = []

    for column_name, column_type in data_frame.dtypes:
        columns.append(ColumnTypeDef(Name=column_name, Type=column_type))

    return columns
