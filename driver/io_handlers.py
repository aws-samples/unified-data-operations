from types import SimpleNamespace
from pyspark.sql import DataFrame
from driver.core import Connection
from driver.driver import get_spark

__CONN_PROVIDER__ = None


def init(connection_provider: callable):
    global __CONN_PROVIDER__
    __CONN_PROVIDER__ = connection_provider


jdbc_drivers = {
    'postgresql': 'org.postgresql.Driver',
    'mysql': 'com.mysql.jdbc'
}


def connection_input_handler(props: SimpleNamespace) -> DataFrame:
    connection: Connection = __CONN_PROVIDER__(props.connection_id)
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
    # return df = get_spark().read.load("examples/src/main/resources/users.parquet")
    pass


def disk_output_handler(model_id: str, df: DataFrame, options: SimpleNamespace):
    pass


def lake_output_handler(model_id: str, df: DataFrame, options: SimpleNamespace):
    partitions = [options.partition_by] if isinstance(options.partition_by, str) else [p for p in options.partition_by]
    df.coalesce(2).write\
        .partitionBy(*partitions) \
        .format(options.stored_as) \
        .option('header', 'true') \
        .save(options.location, mode="overwrite")
