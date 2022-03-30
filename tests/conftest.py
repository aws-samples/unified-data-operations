# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import datetime
import os
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

from driver.util import compile_product, compile_models

DEFAULT_BUCKET = 's3://test-bucket'


@fixture(scope='module')
def fixture_asset_path():
    cwd_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cwd_path, 'assets', 'metafiles')


@fixture(scope='module')
def app_args() -> SimpleNamespace:
    args = SimpleNamespace()
    setattr(args, 'default_data_lake_bucket', DEFAULT_BUCKET)
    return args

@fixture(scope='module')
def movie_schema() -> StructType:
    return StructType([
        StructField('movieId', IntegerType(), True),
        StructField('title', StringType(), True),
        StructField('genres', StringType(), True)
    ])


@fixture(scope='module')
def ratings_schema() -> StructType:
    return StructType([
        StructField('userId', IntegerType(), True),
        StructField('movieId', IntegerType(), True),
        StructField('rating', IntegerType(), True),
        StructField('timestamp', LongType(), True)
    ])


@fixture(scope='module')
def result_schema() -> StructType:
    return StructType([
        StructField('title', StringType(), True),
        StructField('weight_avg', DoubleType(), True),
        StructField('num_votes', IntegerType(), True)
    ])


@fixture(scope='module')
def movies_df(spark_session, movie_schema) -> DataFrame:
    return spark_session.createDataFrame([(1, 'Jumanji(1995)', 'Adventure | Children | Fantasy'),
                                          (2, 'Heat (1995)', 'Action|Crime|Thriller')],
                                         movie_schema)


@fixture(scope='module')
def ratings_df(spark_session, ratings_schema) -> DataFrame:
    return spark_session.createDataFrame([(1, 1, 4, 1256677221),
                                          (2, 1, 4, 1256677222),
                                          (3, 1, 1, 1256677222),
                                          (4, 2, 4, 1256677222)
                                          ], ratings_schema)


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
    return spark_session.createDataFrame([(1, "John", "Doe", 25, "Berlin", "Male"),
                                          (2, "Jane", "Doe", 41, "Berlin", "Female"),
                                          (3, "Maxx", "Mustermann", 30, "Berlin", "Male")
                                          ], person_schema)


@fixture(scope='module')
def transaction_schema() -> StructType:
    return StructType([
        StructField('id', IntegerType(), False),
        StructField('sku', StringType(), True),
        StructField('trx_date', TimestampType(), True),
        StructField('geo', StringType(), True),
        StructField('items', IntegerType(), True)
    ])


@fixture(scope='module')
def transaction_df(spark_session, transaction_schema) -> DataFrame:
    date_field = datetime.datetime.now()
    return spark_session.createDataFrame([(1, "1234", date_field, "EMEA", 25),
                                          (2, "1235", date_field, "EMEA", 41),
                                          (3, "1236", date_field, "US", 30)
                                          ], transaction_schema)


@fixture(scope='module')
def product(app_args, fixture_asset_path):
    return compile_product(fixture_asset_path, app_args)


@fixture(scope='module')
def models(app_args, fixture_asset_path, product):
    return compile_models(fixture_asset_path, product)
