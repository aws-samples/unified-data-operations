# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

import botocore
from mypy_boto3_glue.type_defs import GetDatabasesResponseTypeDef, DatabaseTypeDef, GetTablesResponseTypeDef, \
    TableTypeDef, TableInputTypeDef, StorageDescriptorTypeDef, ColumnTypeDef, DatabaseInputTypeDef
from mypy_boto3_glue.client import Exceptions
from driver.aws import providers
from driver.aws.resolvers import resolve_table_input, resolve_partition_inputs, resolve_database
from driver.task_executor import DataSet

logger = logging.getLogger(__name__)


def drain_data_catalog(data_catalog_id: str):
    glue = providers.get_glue()
    try:
        get_tables_response: GetTablesResponseTypeDef = glue.get_tables(DatabaseName=data_catalog_id)
        for table in get_tables_response.get('TableList'):
            glue.delete_table(DatabaseName=data_catalog_id, Name=table.get('Name'))
    except Exception as enf:
        if enf.__class__.__name__ == 'EntityNotFoundException':
            logger.warning(
                f'Database {data_catalog_id} does not exists in the data catalog. No tables will be deleted.')


def update_data_catalog(ds: DataSet):
    glue = providers.get_glue()
    logger.info(f'--> Updating the data catalog for data product [{ds.product_id}] and model [{ds.model.id}].')

    def upsert_database():
        try:
            rsp: GetDatabasesResponseTypeDef = glue.get_database(Name=ds.product_id)
            # todo: update database with changes
        except Exception as enf:
            if enf.__class__.__name__ == 'EntityNotFoundException':
                # database does not exists yet
                logger.warning(
                    f'Database {ds.product_id} does not exists in the data catalog ({str(enf)}). It is going to be created.')
                # todo: add permissions
                glue.create_database(
                    DatabaseInput=resolve_database(ds))
            else:
                raise enf

    def upsert_table():
        try:
            rsp: GetTablesResponseTypeDef = glue.get_table(DatabaseName=ds.product_id, Name=ds.id)
            # todo: update table
            glue.delete_table(DatabaseName=ds.product_id, Name=ds.id)
            glue.create_table(DatabaseName=ds.product_id, TableInput=resolve_table_input(ds))
            # glue.update_table(DatabaseName=ds.product_id, TableInput=resolve_table_input(ds))
        except Exception as enf:  # EntityNotFoundException
            # table not found
            if enf.__class__.__name__ == 'EntityNotFoundException':
                logger.warning(
                    f'Table [{ds.id}] cannot be found in the catalog schmea [{ds.product_id}]. Table is going to be created.')
                glue.create_table(DatabaseName=ds.product_id, TableInput=resolve_table_input(ds))
            else:
                raise enf
        # rsp: GetTablesResponseTypeDef = glue.get_table(DatabaseName=ds.product_id, Name=ds.id)
        # todo: update partitions
        # todo: register with lakeformation

    def upsert_partitions():
        # entries = resolve_partition_entries(ds)
        # rsp = glue.batch_update_partition(DatabaseName=ds.product_id, TableName=ds.model_id, Entries=entries)
        partition_inputs = resolve_partition_inputs(ds)
        if not partition_inputs:
            return
        rsp = glue.batch_create_partition(DatabaseName=ds.product_id, TableName=ds.id,
                                          PartitionInputList=partition_inputs)
        # rsp = glue.batch_update_partition(DatabaseName=ds.product_id, TableName=ds.id,
        #                                   Entries=partition_inputs)
        if rsp.get('Errors'):
            raise Exception(f"Couldn't update the table [{ds.id}] with the partitions.")
        status_code = rsp.get('ResponseMetadata').get('HTTPStatusCode')
        logger.info(f'Partition upsert response with HTTP Status Code: {str(status_code)}')
        # todo: write a proper error handling here

    upsert_database()
    upsert_table()
    upsert_partitions()  # todo: this is not yet an upsert (just in name but not in implementation)
