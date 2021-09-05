import os
from typing import List, Dict
from mypy_boto3_glue.type_defs import TableTypeDef, StorageDescriptorTypeDef, ColumnTypeDef, SerDeInfoTypeDef, \
    BatchUpdatePartitionRequestEntryTypeDef, PartitionInputTypeDef
from pyspark.sql import DataFrame

from driver.aws.datalake_api import Partition
from driver.aws import datalake_api
from driver.task_executor import DataSet


def resolve_paritions(ds: DataSet) -> List[ColumnTypeDef]:
    return [ColumnTypeDef(name=p, Type=dict(ds.df.dtypes)[p]) for p in ds.partitions]


def resolve_table_type(ds: DataSet) -> str:
    return 'EXTERNAL_TABLE'


def resolve_table_parameters(ds: DataSet) -> Dict[str, str]:
    return {
        "classification": "parquet",
        "compressionType": "none",
        "objectCount": "1",
        "recordCount": ds.df.count(),
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
    return False


def resolve_serde_info(ds: DataSet) -> SerDeInfoTypeDef:
    parquet = SerDeInfoTypeDef(SerializationLibrary='org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                               Parameters={'serialization.format': '1'})
    serdes = {
        'parquet': parquet
    }
    return serdes.get(ds.storage_format)


def resolve_storage_descriptor(ds: DataSet, override_location: str = None) -> StorageDescriptorTypeDef:
    return StorageDescriptorTypeDef(
        Location=override_location if override_location else ds.storage_location,
        InputFormat=resolve_input_format(ds),
        OutputFormat=resolve_output_format(ds),
        Compressed=resolve_compressed(ds),
        SerdeInfo=resolve_serde_info(ds),
        Parameters=resolve_table_parameters(ds),  # todo: partition size
        Columns=resolve_columns(ds.df)
    )


def resolve_columns(data_frame: DataFrame) -> List[ColumnTypeDef]:
    return [ColumnTypeDef(Name=cn, Type=ct) for cn, ct in data_frame.dtypes]


def resolve_table(ds: DataSet) -> TableTypeDef:
    return TableTypeDef(
        Name=ds.model_id,
        DatabaseName=ds.product_id,
        Description=ds.product_description,
        Owner=ds.product_owner,
        PartitionKeys=resolve_paritions(ds),
        TableType=resolve_table_type(ds),
        Parameters=resolve_table_parameters(ds),
        StorageDescriptor=resolve_storage_descriptor(ds)
    )


def resolve_partition_input(partition_location: str, partition_keys: list, ds: DataSet) -> PartitionInputTypeDef:
    return PartitionInputTypeDef(
        Values=partition_keys,
        StorageDescriptor=resolve_storage_descriptor(ds, override_location=partition_location),
        Parameters=resolve_storage_descriptor(ds),
    )


def reshuffle_partitions(prefix: str, partitions: List[Partition]) -> dict:
    partition_list = list()
    partition_dict = dict()
    for po in partitions:
        partition_list.extend(po.get_partition_chain(prefix=prefix))
    for pdict in partition_list:
        partition_dict[pdict.get('location')] = pdict.get('values')
    return partition_dict


def resolve_partition_entries(ds: DataSet) -> List[BatchUpdatePartitionRequestEntryTypeDef]:
    partition_defs = list()
    bucket = os.path.dirname(ds.storage_location.split('//')[1])
    folder = os.path.basename(ds.storage_location.split('//')[1]) or ds.product_id
    ps: List[Partition] = datalake_api.read_partitions(bucket=bucket, container_folder=folder)
    pdict = reshuffle_partitions(bucket, ps)
    for k, v in pdict:
        partition_defs.append(PartitionInputTypeDef(
            PartitionValueList=v,
            PartitionInput=BatchUpdatePartitionRequestEntryTypeDef(k, v, ds)
        ))

    return partition_defs
