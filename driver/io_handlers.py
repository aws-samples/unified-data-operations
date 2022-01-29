import os
from types import SimpleNamespace
from pyspark.sql import DataFrame, DataFrameWriter
from driver.aws import glue_api
from driver.core import Connection, resolve_data_set_id, resolve_data_product_id
from driver.driver import get_spark

__CONN_PROVIDER__ = None
__DATA_PRODUCT_PROVIDER__ = None

from driver.task_executor import DataSet


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
    print(connection.get_jdbc_connection_url(generate_creds=False))
    jdbcDF = get_spark().read.format("jdbc") \
        .option("url", connection.get_jdbc_connection_url(generate_creds=False)) \
        .option("dbtable", props.table) \
        .option("user", connection.principal) \
        .option("password", connection.credential.get_secret_value()) \
        .option("driver", jdbc_drivers.get(connection.type.name)) \
        .load()
    return jdbcDF


def disk_input_handler(props: SimpleNamespace) -> DataFrame:
    #
    # jdbcDF2 = get_spark().read \
    #     .jdbc("jdbc:postgresql:dbserver", "schema.tablename",
    #           properties={"user": "username", "password": "password"})
    # spark.read.load("examples/src/main/resources/people.csv",
    #                 format="csv", sep=";", inferSchema="true", header="true")
    pass


def lake_input_handler(props: SimpleNamespace) -> DataFrame:
    prod_id = resolve_data_product_id(props)
    ds_id = resolve_data_set_id(props)
    data_product_table = __DATA_PRODUCT_PROVIDER__(prod_id, ds_id)
    df = get_spark().read.parquet(data_product_table.storage_location_s3a)
    return df


def disk_output_handler(ds: DataSet, options: SimpleNamespace):
    pass


def lake_output_handler(ds: DataSet):
    output = f"{'s3a://'}{ds.dataset_storage_path.lstrip('/')}"
    # ds.storage_location = f"{'s3://'}{ds.storage_location.lstrip('/')}"
    print(f'writing data product to {output}')
    ds.df.coalesce(2).write \
        .partitionBy(*ds.partitions) \
        .format(ds.stored_as) \
        .mode('overwrite') \
        .option('header', 'true') \
        .save(output)
    # .saveAsTable('test_db.hoppala', path=ds.storage_location)

    # print(f'# partitions after write {ds.df.rdd.getNumPartitions()}')
    #todo: detect the extra schema info that is not defined in the model but provided by transformations
    #todo: add parquet compression support / the glue catalog needs it too
    #todo: add bucket support & also to the glue catalog
    glue_api.update_data_catalog(ds)
