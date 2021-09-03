from typing import List
from mypy_boto3_glue.type_defs import TableTypeDef, StorageDescriptorTypeDef, ColumnTypeDef
from pyspark.sql import DataFrame
from driver.task_executor import DataSet


def resolve_table(table_name: str, data_set: DataSet) -> TableTypeDef:
    return TableTypeDef(
        Name=table_name,
        StorageDescriptor=resolve_storage_descriptor(data_set)
    )


def resolve_storage_descriptor(data_set: DataSet) -> StorageDescriptorTypeDef:
    options = data_set.model.storage.options

    return StorageDescriptorTypeDef(
        Location=options.location,
        Columns=convert_columns(data_set.df)
    )


def convert_columns(data_frame: DataFrame) -> List[ColumnTypeDef]:
    columns: List[ColumnTypeDef] = []

    for column_name, column_type in data_frame.dtypes:
        columns.append(ColumnTypeDef(Name=column_name, Type=column_type))

    return columns
