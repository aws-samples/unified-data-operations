import logging
import os
from urllib.parse import urlparse
from pyspark.sql import DataFrame, DataFrameWriter
from driver.aws import datalake_api, glue_api
from driver.core import Connection, resolve_product_id, resolve_model_id
from driver.driver import get_spark
from driver.task_executor import DataSet
from driver.util import test_property
from .core import ConfigContainer

__CONN_PROVIDER__ = None
__DATA_PRODUCT_PROVIDER__ = None


logger = logging.getLogger(__name__)


def init(connection_provider: callable, data_product_provider: callable):
    global __CONN_PROVIDER__, __DATA_PRODUCT_PROVIDER__
    __CONN_PROVIDER__ = connection_provider
    __DATA_PRODUCT_PROVIDER__ = data_product_provider


jdbc_drivers = {"postgresql": "org.postgresql.Driver", "mysql": "com.mysql.jdbc"}


def connection_input_handler(input_definition: ConfigContainer) -> DataFrame:
    connection: Connection = __CONN_PROVIDER__(input_definition.connection)
    logger.info(f"using input conection: {connection.get_jdbc_connection_url(generate_creds=False)}")
    jdbc_df = (
        get_spark()
        .read.format("jdbc")
        .option("url", connection.get_jdbc_connection_url(generate_creds=False))
        .option("dbtable", input_definition.table)
        .option("user", connection.principal)
        .option("password", connection.credential.get_secret_value())
        .option("driver", jdbc_drivers.get(connection.type.name))
        .option("ssl", connection.ssl)
        .option("sslmode", "require")
        .load()
    )
    return jdbc_df


def file_input_handler(props: ConfigContainer) -> DataFrame:
    def get_type():
        return props.options.type or "parquet"

    def get_separator():
        return props.options.separator or ","

    def get_infer_schema():
        return props.options.infer_schema or "false"

    def get_header():
        return props.options.header or "true"

    parsed = urlparse(props.file)
    scheme = "s3a" if parsed.scheme == "s3" else parsed.scheme
    if parsed.scheme:
        location = f"{scheme}://{parsed.netloc}{parsed.path}"
    else:
        location = f"{parsed.path}"
    logger.info(f"-> [File Input Handler]: reading from {location}")
    if hasattr(props, "options"):
        df = get_spark().read.load(
            location, format=get_type(), sep=get_separator(), inferSchema=get_infer_schema(), header=get_header()
        )
    else:
        df = get_spark().read.load(location)
    return df


def lake_input_handler(io_def: ConfigContainer) -> DataFrame:
    prod_id = resolve_product_id(io_def)
    model_id = resolve_model_id(io_def)
    data_product_table = __DATA_PRODUCT_PROVIDER__(prod_id, model_id)
    df = get_spark().read.parquet(data_product_table.storage_location_s3a)
    return df


def file_output_handler(ds: DataSet, options: ConfigContainer):
    raise NotImplementedError


def resolve_compression(ds: DataSet):
    # todo: parse this into an enum
    # none, uncompressed, snappy, gzip, lzo, brotli, lz4,
    if test_property(ds, "model.storage.options.compression"):
        return ds.model.storage.options.compression
    else:
        return "snappy"


def resolve_coalesce(ds: DataSet):
    if test_property(ds, "model.storage.options.coalesce"):
        return ds.model.storage.options.coalesce
    else:
        return 2


def resolve_header(ds: DataSet):
    if test_property(ds, "model.storage.options.skip_first_row"):
        return ds.model.storage.options.skip_first_row
    else:
        return "true"


def lake_output_handler(ds: DataSet):
    output = f"{'s3a://'}{ds.dataset_storage_path.lstrip('/')}"
    logging.info(f"-> [Lake Output Handler]: writing data product to: {output}")
    ds.df.coalesce(resolve_coalesce(ds)).write.partitionBy(*ds.partitions or []).format(ds.storage_format).mode(
        "overwrite"
    ).option("header", resolve_header(ds)).option("compression", resolve_compression(ds)).save(output)

    datalake_api.tag_files(ds.storage_location, ds.path, ds.all_tags)

    # print(f'# partitions after write {ds.df.rdd.getNumPartitions()}')
    # todo: recheck coalesce value
    # todo: add parquet compression support / the glue catalog needs it too
    # todo: add bucket support & also to the glue catalog
    glue_api.update_data_catalog(ds)
