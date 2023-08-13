# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import datetime
from driver import ConfigContainer

import pytest
from pyspark.sql.functions import lit, col

from driver.core import ValidationException
from driver.processors import past_validator, future_validator, unique_validator, regexp_validator, null_validator, \
    freshness_validator


def test_past_validator(spark_session, transaction_df):
    past_validator(transaction_df, 'trx_date', ConfigContainer(threshold=5, time_unit='hours'))
    past_validator(transaction_df, 'trx_date', ConfigContainer(threshold=5))
    past_validator(transaction_df, 'trx_date', ConfigContainer())
    past_validator(transaction_df, 'trx_date')
    with pytest.raises(ValidationException) as vex:
        updf = transaction_df.withColumn('trx_date', lit(datetime.datetime.now() + datetime.timedelta(days=5)))
        updf.show()
        past_validator(updf, 'trx_date', ConfigContainer(threshold=5, time_unit='hours'))


def test_future_validator(spark_session, transaction_df):
    transaction_df.show()
    future_validator(transaction_df, 'trx_date', ConfigContainer(threshold=5, time_unit='hours'))
    future_validator(transaction_df, 'trx_date', ConfigContainer(threshold=5))
    with pytest.raises(ValidationException):
        future_validator(transaction_df, 'trx_date', ConfigContainer())
    with pytest.raises(ValidationException):
        future_validator(transaction_df, 'trx_date')
    updf = transaction_df.withColumn('trx_date', lit(datetime.datetime.now() + datetime.timedelta(days=5)))
    updf.show()
    future_validator(updf, 'trx_date')


def test_unique_validator(spark_session, transaction_df, transaction_schema):
    unique_validator(transaction_df, 'sku')
    with pytest.raises(ValidationException):
        new_row = spark_session.createDataFrame([(4, "1236", datetime.datetime.now(), "US", 30)], transaction_schema)
        appended = transaction_df.union(new_row)
        appended.show()
        unique_validator(appended, 'sku')


def test_regexp_validator(spark_session, transaction_df, transaction_schema):
    regexp_validator(transaction_df, 'geo', ConfigContainer(value='^EMEA|US$'))
    with pytest.raises(ValidationException):
        new_row = spark_session.createDataFrame([(4, "1237", datetime.datetime.now(), "APJ", 30)], transaction_schema)
        appended = transaction_df.union(new_row)
        regexp_validator(appended, 'geo', ConfigContainer(value='^EMEA|US$'))


def test_null_validator(spark_session, transaction_df, transaction_schema):
    null_validator(transaction_df, 'geo')
    with pytest.raises(ValidationException):
        new_row = spark_session.createDataFrame([(4, "1237", datetime.datetime.now(), None, 30)], transaction_schema)
        appended = transaction_df.union(new_row)
        null_validator(appended, 'geo')


def test_freshness_validator(spark_session, transaction_df, transaction_schema):
    freshness_validator(transaction_df, 'trx_date', ConfigContainer(threshold=1, time_unit='minutes'))
    with pytest.raises(ValidationException):
        trx_date = datetime.datetime.now() - datetime.timedelta(minutes=10)
        upd_df = transaction_df.withColumn("trx_date", lit(trx_date))
        freshness_validator(upd_df, 'trx_date', ConfigContainer(threshold=1, time_unit='minutes'))

    freshness_validator(transaction_df, 'trx_date', ConfigContainer(threshold=1, time_unit='minutes', group_by='geo'))

    with pytest.raises(ValidationException):
        trx_date = datetime.datetime.now() - datetime.timedelta(minutes=10)
        new_row = spark_session.createDataFrame([(4, "1237", trx_date, "APJ", 30)], transaction_schema)
        appended = transaction_df.union(new_row)
        freshness_validator(appended, 'trx_date', ConfigContainer(threshold=1, time_unit='minutes', group_by='geo'))
