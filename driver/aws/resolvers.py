# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from typing import List, Dict
from mypy_boto3_glue.type_defs import TableTypeDef, StorageDescriptorTypeDef, ColumnTypeDef, SerDeInfoTypeDef, \
    BatchUpdatePartitionRequestEntryTypeDef, PartitionInputTypeDef, TableInputTypeDef, DatabaseInputTypeDef
from pyspark.sql import DataFrame

from driver.aws.datalake_api import Partition
from driver.aws import datalake_api
from driver.task_executor import DataSet
from driver.util import filter_list_by_id, safe_get_property


def resolve_partitions(ds: DataSet) -> List[ColumnTypeDef]:
    return [ColumnTypeDef(Name=p, Type=dict(ds.df.dtypes)[p]) for p in ds.partitions]


def resolve_table_type(ds: DataSet) -> str:
    return 'EXTERNAL_TABLE'


def resolve_table_parameters(ds: DataSet) -> Dict[str, str]:
    return {
        "classification": "parquet",
        "compressionType": "none",
        "objectCount": "1",
        "recordCount": str(ds.df.count()),
        "typeOfData": "file"
    }


def resolve_input_format(ds: DataSet) -> str:
    formats = {
        'parquet': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    }
    return formats.get(ds.storage_format)


def resolve_output_format(ds: DataSet) -> str:
    formats = {
        'parquet': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    }
    return formats.get(ds.storage_format)


def resolve_compressed(ds: DataSet) -> bool:
    # return str(False).lower()
    return False


def resolve_serde_info(ds: DataSet) -> SerDeInfoTypeDef:
    parquet = SerDeInfoTypeDef(SerializationLibrary='org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                               Parameters={'serialization.format': '1'})
    serdes = {
        'parquet': parquet
    }
    return serdes.get(ds.storage_format)


def resolve_storage_descriptor(ds: DataSet, override_location: str = None) -> StorageDescriptorTypeDef:
    if override_location:
        path = f's3://{os.path.join(override_location, "")}'
    else:
        path = f"s3://{ds.dataset_storage_path.lstrip('/')}"
    return StorageDescriptorTypeDef(
        Location=path,
        InputFormat=resolve_input_format(ds),
        OutputFormat=resolve_output_format(ds),
        Compressed=resolve_compressed(ds),
        NumberOfBuckets=-1,  # todo: check how to calculate this.
        SerdeInfo=resolve_serde_info(ds),
        Parameters=resolve_table_parameters(ds),  # todo: partition size
        Columns=resolve_columns(ds)
    )


def resolve_columns(ds: DataSet) -> List[ColumnTypeDef]:
    def lookup(column_name):
        if not hasattr(ds.model, 'columns'):
            return str()
        model_column = filter_list_by_id(ds.model.columns, column_name)
        if hasattr(model_column, 'name'):
            return f"{safe_get_property(model_column, 'name')}: {safe_get_property(model_column, 'description')}"
        else:
            return str()

    return [ColumnTypeDef(Name=cn, Type=ct, Comment=lookup(cn)) for cn, ct in ds.df.dtypes if cn not in ds.partitions]


def resolve_table(ds: DataSet) -> TableTypeDef:
    return TableTypeDef(
        Name=ds.model_name,
        DatabaseName=ds.product_id,
        Description=ds.model_description,
        Owner=ds.product_owner,
        PartitionKeys=resolve_partitions(ds),
        TableType=resolve_table_type(ds),
        Parameters=resolve_table_parameters(ds),
        StorageDescriptor=resolve_storage_descriptor(ds)
    )


def resolve_table_input(ds: DataSet) -> TableInputTypeDef:
    return TableInputTypeDef(
        Name=ds.id,
        Description=f'{ds.model_name}: {ds.model_description}',
        Owner=ds.product_owner or str(),
        PartitionKeys=resolve_partitions(ds),
        TableType='EXTERNAL_TABLE',
        Parameters=resolve_table_parameters(ds),
        StorageDescriptor=resolve_storage_descriptor(ds)
    )


def resolve_partition_input(partition_location: str, partition_values: list, ds: DataSet) -> PartitionInputTypeDef:
    return PartitionInputTypeDef(
        Values=partition_values,
        StorageDescriptor=resolve_storage_descriptor(ds, override_location=partition_location),
        Parameters=resolve_table_parameters(ds),
    )


def reshuffle_partitions(prefix: str, partitions: List[Partition]) -> dict:
    partition_list = list()
    partition_dict = dict()
    for po in partitions:
        partition_list.extend(po.get_partition_chain(prefix=prefix))
    for pdict in partition_list:
        # if pdict.get('location') not in ['glue-job-test-destination-bucket/person/gender=Female',
        #                                  'glue-job-test-destination-bucket/person/gender=Male']:
        #     #todo: remove this ugly hack
        partition_dict[pdict.get('location')] = {
            'keys': pdict.get('keys'),
            'values': pdict.get('values')
        }
    return partition_dict


def resolve_partition_inputs(ds: DataSet, format_for_update: bool = False) -> List[PartitionInputTypeDef]:
    bucket = ds.storage_location.lstrip('/').split('/')[0]
    folder = '/'.join(ds.dataset_storage_path.lstrip('/').split('/')[1:])
    ps: List[Partition] = datalake_api.read_partitions(bucket=bucket, container_folder=folder)
    pdict = reshuffle_partitions(os.path.join(bucket, folder), ps)
    partition_defs = list()
    for k, v in pdict.items():
        partition_values = v.get('values')
        if format_for_update:
            entry = {'PartitionValueList': v.get('values'),
                     'PartitionInput': resolve_partition_input(partition_location=k, partition_values=partition_values,
                                                               ds=ds)}
            partition_defs.append(entry)
        else:
            partition_defs.append(
                resolve_partition_input(partition_location=k, partition_values=partition_values, ds=ds))
    return partition_defs


def resolve_partition_entries(ds: DataSet) -> List[BatchUpdatePartitionRequestEntryTypeDef]:
    partition_defs = list()
    bucket = ds.storage_location.lstrip('/').split('/')[0]
    folder = '/'.join(ds.dataset_storage_path.lstrip('/').split('/')[1:])
    ps: List[Partition] = datalake_api.read_partitions(bucket=bucket, container_folder=folder)
    pdict = reshuffle_partitions(bucket, ps)
    for k, v in pdict.items():
        partition_defs.append(BatchUpdatePartitionRequestEntryTypeDef(
            PartitionValueList=v.get('values'),
            PartitionInput=resolve_partition_input(partition_location=k, partition_values=v.get('values'), ds=ds)
        ))
    return partition_defs


def resolve_database(ds: DataSet) -> DatabaseInputTypeDef:
    return DatabaseInputTypeDef(Name=ds.product_id, Description=ds.product.description or str())
