import quinn
from pyspark.sql import DataFrame

from driver import common
from driver.task_executor import DataSet
from quinn.dataframe_validator import (
    DataFrameMissingStructFieldError,
    DataFrameMissingColumnError,
    DataFrameProhibitedColumnError
)


def null_validator(df: DataFrame, col_name: str, cfg: any = None):
    df.withColumn(f'is_{col_name}_null_or_blank', quinn.F.col(col_name).isNullOrBlank())


def unique_validator(df: DataFrame, col_name: str, cfg: any = None):
    col = df.select(col_name)
    if col.distinct().count() != col.count():
        raise common.ValidationError(f'Columns: {col_name} is expected to be unique.')


constraint_validators = {
    "not_null": null_validator,
    "unique": unique_validator
}


def schema_validator(ds: DataSet):
    try:
        if ds.model:
            ds_schema = common.remap_schema(ds)
            quinn.validate_schema(ds.df, ds_schema)
    except (DataFrameMissingColumnError, DataFrameMissingStructFieldError, DataFrameProhibitedColumnError) as ex:
        raise common.ValidationError(f'Schema Validation Error: {str(ex)} of type: {type(ex).__name__}')
    return ds


def constraint_processor(ds: DataSet):
    if not hasattr(ds, 'model'):
        return

    for col in ds.model.columns:
        if not hasattr(col, 'constraints'):
            continue
        constraints = [c.type for c in col.constraints]
        for c in constraints:
            cv = constraint_validators.get(c)
            if cv:
                cv(ds.df, col.id, col.constraints.get(c))
    return ds


def anonymizer(ds: DataSet):
    pass
