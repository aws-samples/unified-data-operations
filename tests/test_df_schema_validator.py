import os
from time import time
from datetime import datetime
from types import SimpleNamespace

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, unix_timestamp, lit

from driver.core import DataSet, DataProduct, SchemaValidationException
from driver.processors import schema_checker
from driver.util import compile_product, compile_models, filter_list_by_id


def test_df_schema_validator(movies_df: DataFrame, product, models):
    movie_model = filter_list_by_id(models, 'movie')
    dp = DataProduct(id=product.id, description=product.description, owner=product.owner)
    ds = DataSet(id='movie', df=movies_df, model=movie_model, product=dp)
    ds = schema_checker(ds)


def test_df_schema_validator_missing_fields(movies_df: DataFrame, product, models):
    movie_model = filter_list_by_id(models, 'movie')
    dp = DataProduct(id=product.id, description=product.description, owner=product.owner)
    ds = DataSet(id='movie', df=movies_df.drop(col('genres')), model=movie_model, product=dp)
    with pytest.raises(SchemaValidationException):
        ds = schema_checker(ds)


def test_df_schema_validator_extra_fields_lazy(movies_df: DataFrame, product, models):
    movie_model = filter_list_by_id(models, 'movie')
    dp = DataProduct(id=product.id, description=product.description, owner=product.owner)
    timestamp = datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')
    df = movies_df.withColumn('time', unix_timestamp(lit(timestamp), 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
    ds = DataSet(id='movie', df=df, model=movie_model, product=dp)
    ds = schema_checker(ds)
    df.show()


def test_df_schema_validator_extra_fields_strict(movies_df: DataFrame, product, fixture_asset_path):
    models = compile_models(fixture_asset_path, product, def_file_name='model_strict_validation.yml')
    movie_model = filter_list_by_id(models, 'movie')
    dp = DataProduct(id=product.id, description=product.description, owner=product.owner)
    timestamp = datetime.fromtimestamp(time()).strftime('%Y-%m-%d %H:%M:%S')
    df = movies_df.withColumn('time', unix_timestamp(lit(timestamp), 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
    ds = DataSet(id='movie', df=df, model=movie_model, product=dp)
    df.show()
    with pytest.raises(SchemaValidationException) as exc:
        ds = schema_checker(ds)