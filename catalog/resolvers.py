from typing import List
from mypy_boto3_glue.type_defs import TableTypeDef, TableInputTypeDef, StorageDescriptorTypeDef, ColumnTypeDef
from pyspark.sql import DataFrame
from driver.task_executor import DataSet


def resolve_table(table_name: str, data_set: DataSet) -> TableTypeDef:
    return TableInputTypeDef(
        Name=table_name,
        StorageDescriptor=resolve_storage_descriptor(data_set)
    )


def resolve_storage_descriptor(data_set: DataSet) -> StorageDescriptorTypeDef:
    options = data_set.model.options

    return StorageDescriptorTypeDef(
        Location=convert_location(options),
        Columns=convert_columns(data_set.df)
    )


def convert_format(options: dict):
    return options.get('stored_as')


def convert_location(options: dict):
    return options.get('location')


def convert_columns(data_frame: DataFrame) -> List[ColumnTypeDef]:
    columns: List[ColumnTypeDef] = []

    for column_name, column_type in data_frame.dtypes:
        columns.append(ColumnTypeDef(Name=column_name, Type=column_type))

    return columns
