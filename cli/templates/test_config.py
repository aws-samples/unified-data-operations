from types import SimpleNamespace
from pyspark.sql import DataFrame
from pytest import fixture
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
    LongType,
    DoubleType, TimestampType
)

DEFAULT_BUCKET = 's3://test-bucket'


@fixture
def app_args() -> SimpleNamespace:
    args = SimpleNamespace()
    setattr(args, 'default_data_lake_bucket', DEFAULT_BUCKET)
    return args


@fixture(scope='module')
def person_schema() -> StructType:
    return StructType([
        StructField('id', IntegerType(), False),
        StructField('first_name', StringType(), True),
        StructField('last_name', StringType(), True),
        StructField('age', IntegerType(), True),
        StructField('city', StringType(), True),
        StructField('gender', StringType(), True),
    ])


@fixture(scope='module')
def person_df(spark_session, person_schema) -> DataFrame:
    return spark_session.createDataFrame([(1, "John", "Doe", 25, "Berlin", "male"),
                                          (2, "Jane", "Doe", 41, "Berlin", "female"),
                                          (3, "Maxx", "Mustermann", 30, "Berlin", "male")
                                          ], person_schema)
