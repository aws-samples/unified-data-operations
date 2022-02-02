import logging
import os
from types import SimpleNamespace
from urllib.parse import urlparse

from pyspark.sql import DataFrame, DataFrameWriter
from driver.aws import glue_api, datalake_api
from driver.core import Connection, resolve_data_set_id, resolve_data_product_id
from driver.driver import get_spark
from driver.task_executor import DataSet

__CONN_PROVIDER__ = None
__DATA_PRODUCT_PROVIDER__ = None
logger = logging.getLogger(__name__)


def init(connection_provider: callable, data_product_provider: callable):
    global __CONN_PROVIDER__, __DATA_PRODUCT_PROVIDER__
    __CONN_PROVIDER__ = connection_provider
    __DATA_PRODUCT_PROVIDER__ = data_product_provider


jdbc_drivers = {
    'postgresql': 'org.postgresql.Driver',
    'mysql': 'com.mysql.jdbc'
}


def connection_input_handler(props: SimpleNamespace) -> DataFrame:
    connection: Connection = __CONN_PROVIDER__(props.connection)
    logger.info(connection.get_jdbc_connection_url(generate_creds=False))
    jdbcDF = get_spark().read.format("jdbc") \
        .option("url", connection.get_jdbc_connection_url(generate_creds=False)) \
        .option("dbtable", props.table) \
        .option("user", connection.principal) \
        .option("password", connection.credential.get_secret_value()) \
        .option("driver", jdbc_drivers.get(connection.type.name)) \
        .load()
    return jdbcDF


def file_input_handler(props: SimpleNamespace) -> DataFrame:
    def get_type():
        return props.options.type or 'parquet'

    def get_separator():
        return props.options.separator or ','

    def get_infer_schema():
        return props.options.infer_schema or 'false'

    def get_header():
        return props.options.header or 'true'

    parsed = urlparse(props.file)
    scheme = 's3a' if parsed.scheme == 's3' else parsed.scheme
    if parsed.scheme:
        location = f'{scheme}://{parsed.netloc}{parsed.path}'
    else:
        location = f'{parsed.path}'
    logger.info(f'-> [File Input Handler]: reading from {location}')
    if hasattr(props, 'options'):
        df = get_spark().read.load(location, format=get_type(), sep=get_separator(),
                                   inferSchema=get_infer_schema(), header=get_header())
    else:
        df = get_spark().read.load(location)
    return df


def lake_input_handler(io_def: SimpleNamespace) -> DataFrame:
    prod_id = resolve_data_product_id(io_def)
    ds_id = resolve_data_set_id(io_def)
    data_product_table = __DATA_PRODUCT_PROVIDER__(prod_id, ds_id)
    df = get_spark().read.parquet(data_product_table.storage_location_s3a)
    return df


def file_output_handler(ds: DataSet, options: SimpleNamespace):
    pass


def lake_output_handler(ds: DataSet):
    output = f"{'s3a://'}{ds.dataset_storage_path.lstrip('/')}"
    # ds.storage_location = f"{'s3://'}{ds.storage_location.lstrip('/')}"
    logging.info(f'-> [Lake Output Handler]: writing data product to: {output}')
    ds.df.coalesce(2).write \
        .partitionBy(*ds.partitions or []) \
        .format(ds.storage_format) \
        .mode('overwrite') \
        .option('header', 'true') \
        .save(output)
    # .saveAsTable('test_db.hoppala', path=ds.storage_location)

    datalake_api.tag_files(ds.storage_location, ds.storage_path, ds.all_tags)

    # print(f'# partitions after write {ds.df.rdd.getNumPartitions()}')
    # todo: recheck coalesce value
    # todo: detect the extra schema info that is not defined in the model but provided by transformations
    # todo: add parquet compression support / the glue catalog needs it too
    # todo: add bucket support & also to the glue catalog
    glue_api.update_data_catalog(ds)
