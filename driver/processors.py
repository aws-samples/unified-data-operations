# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import hashlib
import logging
import re
from datetime import datetime, timedelta
from typing import List

from driver import common
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, lit, udf, hash, to_date, row_number
from pyspark.sql.types import StringType, StructField, TimestampType
from pyspark.ml.feature import Bucketizer
from driver.core import ValidationException, SchemaValidationException
from driver.task_executor import DataSet

from driver.util import check_property

logger = logging.getLogger(__name__)


def null_validator(df: DataFrame, col_name: str, cfg: any = None):
    # null_value_ratio = df.select(count(when(col(col_name).isNull(), True)) / count(lit(1)).alias('count')) \
    #     .first()[0]
    # ('not_null', self.column, null_value_ratio <= self.threshold, self.threshold, null_value_ratio
    col = df.select(col_name)
    if col.filter((col[col_name].isNull()) | (col[col_name] == "")).count() > 0:
        raise ValidationException(f'Column: {col_name} is expected to be not null.')


def regexp_validator(df: DataFrame, col_name: str, cfg: any = None):
    if not hasattr(cfg, 'value'):
        raise ValidationException(f'Column {col_name} has regexp constraint validator, but no value option provided.')
    col = df.select(col_name)
    if col.count() != col.filter(col[col_name].rlike(cfg.value)).count():
        raise ValidationException(f"Column: [{col_name}] doesn't match regexp: {cfg.value}")


def unique_validator(df: DataFrame, col_name: str, cfg: any = None):
    col = df.select(col_name)
    if col.distinct().count() != col.count():
        raise ValidationException(f'Column: {col_name} is expected to be unique.')


def resolve_time_delta(cfg):
    if hasattr(cfg, 'time_unit'):
        if cfg.time_unit == 'minutes':
            return timedelta(minutes=cfg.threshold)
        elif cfg.time_unit == 'hours':
            return timedelta(hours=cfg.threshold)
        elif cfg.time_unit == 'days':
            return timedelta(days=cfg.threshold)
        elif cfg.time_unit == 'weeks':
            return timedelta(weeks=cfg.threshold)
        elif cfg.time_unit == 'seconds':
            return timedelta(seconds=cfg.threshold)
    else:
        return timedelta(minutes=cfg.threshold)


def past_validator(df: DataFrame, col_name: str, cfg: any = None):
    now = datetime.now()
    if cfg and hasattr(cfg, 'threshold'):
        now = now + resolve_time_delta(cfg)
    count = df.filter(df["trx_date"].cast(TimestampType()) >= lit(now)).count()
    if count > 0:
        raise ValidationException(f'Column {col_name} has values in the future (beyond {now}).')


def future_validator(df: DataFrame, col_name: str, cfg: any = None):
    now = datetime.now()
    if cfg and hasattr(cfg, 'threshold'):
        now = now - resolve_time_delta(cfg)
    count = df.filter(df["trx_date"].cast(TimestampType()) <= lit(now)).count()
    if count > 0:
        raise ValidationException(f'Column {col_name} has values in the past (before {now}).')


def freshness_validator(df: DataFrame, col_name: str, cfg: any = None):
    regex = re.compile('seconds|minutes|hours|days|weeks', re.I)
    if not hasattr(cfg, 'threshold') or not hasattr(cfg, 'time_unit') or not regex.match(str(cfg.time_unit)):
        raise ValidationException(
            f'[threshold] and [time_unit] options must be specified. Time units shoudl have one of the following values: seconds|minutes|hours|days|weeks.')
    if hasattr(cfg, 'group_by'):
        # df.withColumn("rn", row_number().over(Window.partitionBy(cfg.group_by).orderBy(col(col_name).desc())))
        # df = df.filter(col("rn") == 1).drop("rn")
        res_df = df.select(col(col_name), col(cfg.group_by)).withColumn('rn', row_number().over(
            Window.partitionBy(cfg.group_by).orderBy(col(col_name).desc()))).filter(col('rn') == 1).drop('rn')
        threshold = datetime.now() - resolve_time_delta(cfg)
        for row in res_df.collect():
            if row[col_name] < threshold:
                raise ValidationException(
                    f'The most recent row for group [{cfg.group_by}] is older ({row[col_name]}) than the threshold ({threshold}).')
    else:
        threshold = datetime.now() - resolve_time_delta(cfg)
        most_recent = df.select(col(col_name)).orderBy(col(col_name).desc()).first()[col_name]
        if most_recent < threshold:
            raise ValidationException(f'The most recent row is older ({most_recent}) than the threshold ({threshold}).')


def min_validator(df: DataFrame, col_name: str, cfg: any = None):
    # todo: implement min validator
    pass


def max_validator(df: DataFrame, col_name: str, cfg: any = None):
    # todo: implement max validator
    pass


constraint_validators = {
    "not_null": null_validator,
    "unique": unique_validator,
    "regexp": regexp_validator,
    "past": past_validator,
    "future": future_validator,
    "freshness": freshness_validator
}


def hasher(df: DataFrame, col_name: str, cfg: any = None) -> DataFrame:
    # todo: implement salting
    return df.withColumn(col_name, hash(col(col_name)))


def encrypt(df: DataFrame, col_name: str, cfg: any = None) -> DataFrame:
    # todo: implement key handling + kms
    def encrypt_f(value: object, key: str = None):
        if key:
            return hashlib.sha256(str(value).encode() + key.encode()).hexdigest()
        else:
            return hashlib.sha256(str(value).encode()).hexdigest()

    encrypt_udf = udf(encrypt_f, StringType())
    return df.withColumn(col_name, encrypt_udf(col_name, lit(cfg.key if hasattr(cfg, 'key') else None)))


def skip_column(df: DataFrame, col_name: str, cfg: any = None) -> DataFrame:
    return df.drop(col(col_name))


def rename_col(df: DataFrame, col_name: str, cfg: any = None) -> DataFrame:
    # todo: update the schema for the dataset or remove this one
    return df.withColumnRenamed(col_name, cfg.name)


def bucketize(df: DataFrame, col_name: str, cfg: any = None) -> DataFrame:
    buckets = cfg.buckets.__dict__
    bucket_labels = dict(zip(range(len(buckets.values())), buckets.values()))
    bucket_splits = [float(split) for split in buckets.keys()]
    bucket_splits.append(float('Inf'))

    bucketizer = Bucketizer(splits=bucket_splits, inputCol=col_name, outputCol="tmp_buckets")
    bucketed = bucketizer.setHandleInvalid("keep").transform(df)

    udf_labels = udf(lambda x: bucket_labels[x], StringType())
    bucketed = bucketed.withColumn(col_name, udf_labels("tmp_buckets"))
    bucketed = bucketed.drop(col('tmp_buckets'))

    return bucketed


built_in_transformers = {
    'anonymize': hasher,
    'encrypt': encrypt,
    'skip': skip_column,
    'bucketize': bucketize,
    'rename_column': rename_col
}


def find_schema_delta(ds: DataSet) -> List[StructField]:
    def lookup(name, schema_list):
        return next(filter(lambda rsf: rsf.name == name, schema_list))

    if check_property(ds, 'model.columns'):
        required_schema = common.remap_schema(ds)
        data_frame_fields = [{'name': x.name, 'type': x.dataType} for x in ds.df.schema]
        required_schema_fields = [{'name': x.name, 'type': x.dataType} for x in required_schema]
        delta_fields = [x for x in required_schema_fields if x not in data_frame_fields]
        return [lookup(x.get('name'), required_schema) for x in delta_fields]
    else:
        return None


def type_caster(ds: DataSet):
    try:
        mismatched_fields = find_schema_delta(ds)
        for mismatched_field in mismatched_fields or []:
            logger.info(
                f'--> typecasting [{mismatched_field.name}] to type: [{mismatched_field.dataType.typeName()}] in [{ds.id}]')
            field_in_df = next(iter([f for f in ds.df.schema.fields if f.name == mismatched_field.name]), None)
            if field_in_df:
                ds.df = ds.df.withColumn(mismatched_field.name,
                                         col(mismatched_field.name).cast(mismatched_field.dataType.typeName()))
        return ds
    except Exception as e:
        raise e


def schema_checker(ds: DataSet):
    if check_property(ds, 'model.columns'):
        logger.info(
            f'-> checking schema for dataset [{ds.id}] with model id: [{ds.model.id}]. Data frame columns: {len(ds.df.columns)}')
        missing_fields = find_schema_delta(ds)
        if missing_fields:
            raise SchemaValidationException(
                f'The following fields are missing from the data set [{ds.id}]: {missing_fields}. '
                f'Current schema: {ds.df.schema}',
                ds)
        if hasattr(ds, 'model') and hasattr(ds.model, 'validation') and ds.model.validation == 'strict':
            if not hasattr(ds, 'df'):
                raise SchemaValidationException(f'The dataset [{ds.id}] is missing a dataframe with strict validation',
                                                ds)
            if len(ds.df.columns) != len(ds.model.columns):
                xtra = set(ds.df.columns) - set([x.id for x in ds.model.columns])
                raise SchemaValidationException(
                    f'The dataset [{ds.id}] has a dataframe with more columns ({xtra}) than stated in the model', ds)
    return ds


def razor(ds: DataSet):
    if hasattr(ds.model, 'xtra_columns') and ds.model.xtra_columns == 'raze':
        xtra_columns = list(set(ds.df.columns) - set([x.id for x in ds.model.columns]))
        ds.df = ds.df.drop(*xtra_columns)
    return ds


def constraint_processor(ds: DataSet):
    if not check_property(ds, 'model.columns'):
        return ds

    for col in ds.model.columns:
        if not hasattr(col, 'constraints'):
            continue
        constraint_types = [c.type for c in col.constraints]
        for ctype in constraint_types:
            cvalidator = constraint_validators.get(ctype)
            if cvalidator:
                constraint = next(iter([co for co in col.constraints if co.type == ctype]), None)
                constraint_opts = constraint.options if hasattr(constraint, 'options') else None
                cvalidator(ds.df, col.id, constraint_opts)
    return ds


def transformer_processor(data_set: DataSet):
    """
    Will run a prebuilt a transformation on each and every column of the model.
    :param data_set: the data set that contains the data frame;
    :return: the data set with the processed data frame
    """
    if not check_property(data_set, 'model.columns'):
        return data_set
    for col in data_set.model.columns:
        if not hasattr(col, 'transform'):
            continue
        transformers = [t.type for t in col.transform]
        for trsfrm_type in transformers:
            tcall = built_in_transformers.get(trsfrm_type)
            if tcall:
                trsfrm = next(iter([to for to in col.transform if to.type == trsfrm_type]), None)
                trsfm_opts = trsfrm.options if trsfrm and hasattr(trsfrm, 'options') else None
                data_set.df = tcall(data_set.df, col.id, trsfm_opts)
    return data_set
