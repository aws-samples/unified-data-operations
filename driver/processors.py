import hashlib

import quinn
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import col, count, when, lit, udf
from pyspark.sql.types import StringType
from pyspark.ml.feature import Bucketizer

from driver import common
from driver.task_executor import DataSet
from quinn.dataframe_validator import (
    DataFrameMissingStructFieldError,
    DataFrameMissingColumnError,
    DataFrameProhibitedColumnError
)


def null_validator(df: DataFrame, col_name: str, cfg: any = None):
    # null_value_ratio = df.select(count(when(col(col_name).isNull(), True)) / count(lit(1)).alias('count')) \
    #     .first()[0]
    # ('not_null', self.column, null_value_ratio <= self.threshold, self.threshold, null_value_ratio

    if df.filter((df[col_name].isNull()) | (df[col_name] == "")).count() > 0:
        raise common.ValidationException(f'Column: {col_name} is expected to be not null.')


def regexp_validator(df: DataFrame, col_name: str, cfg: any = None):
    if df.select(col(col_name)).count() != df.select(col(col_name).rlike(cfg.value)).count():
        raise common.ValidationException(f"Column: {col_name} doesn't match regexp: {cfg.value}")


def unique_validator(df: DataFrame, col_name: str, cfg: any = None):
    col = df.select(col_name)
    if col.distinct().count() != col.count():
        raise common.ValidationException(f'Column: {col_name} is expected to be unique.')


constraint_validators = {
    "not_null": null_validator,
    "unique": unique_validator,
    "regexp": regexp_validator
}


def hash(df: DataFrame, col_name: str, cfg: any = None) -> DataFrame:
    return df.withColumn(col_name, F.hash(col(col_name)))


def encrypt(df: DataFrame, col_name: str, cfg: any = None) -> DataFrame:
    def encrypt_f(key: str, value: object):
        if key:
            return hashlib.sha256(str(value).encode() + key.encode()).hexdigest()
        else:
            return hashlib.sha256(str(value).encode()).hexdigest()

    encrypt_udf = F.udf(encrypt_f, StringType())
    return df.withColumn(col_name, encrypt_udf(lit(cfg.key), col_name))


def skip_column(df: DataFrame, col_name: str, cfg: any = None) -> DataFrame:
    return df.drop(col(col_name))


def rename_col(df: DataFrame, col_name: str, cfg: any = None) -> DataFrame:
    return df.withColumnRenamed(col_name, cfg.name)


def bucketize(df: DataFrame, col_name: str, cfg: any = None) -> DataFrame:
    bucketizer = Bucketizer(splits=[0, 6, 18, 60, float('Inf')], inputCol=col_name, outputCol="buckets")
    return bucketizer.setHandleInvalid("keep").transform(df)


built_in_transformers = {
    'anonymize': hash,
    'encrypt': encrypt,
    'skip': skip_column,
    'rename_column': rename_col
}


def schema_validator(ds: DataSet):
    try:
        if ds.model:
            ds_schema = common.remap_schema(ds)
            quinn.validate_schema(ds.df, ds_schema)
    except (DataFrameMissingColumnError, DataFrameMissingStructFieldError, DataFrameProhibitedColumnError) as ex:
        raise common.ValidationException(f'Schema Validation Error: {str(ex)} of type: {type(ex).__name__}')
    return ds


def constraint_processor(ds: DataSet):
    if not hasattr(ds, 'model'):
        return ds

    for col in ds.model.columns:
        if not hasattr(col, 'constraints'):
            continue
        constraints = [c.type for c in col.constraints]
        for c in constraints:
            cv = constraint_validators.get(c)
            if cv:
                cv(ds.df, col.id, next(iter([co for co in col.constraints if co.type == c]), None))
    return ds


def transformer_processor(ds: DataSet):
    if not hasattr(ds, 'model'):
        return ds
    for col in ds.model.columns:
        if not hasattr(col, 'transform'):
            continue
        transformers = [t.type for t in col.transform]
        for t in transformers:
            tc = built_in_transformers.get(t)
            if t:
                ds.df = tc(ds.df, col.id, next(iter([to for to in col.transform if to.type == t]), None))
    return ds
