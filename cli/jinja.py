import os
import time
import random
from faker import Faker
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from .fakers import get_fake_value_by_spark_type, get_text_fakers
from driver.util import convert_model_type_to_spark_type
from driver.core import ConfigContainer, resolve_data_set_id, resolve_product_id, resolve_model_id
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NumericType,
    ShortType,
    TimestampType,
    StringType,
)

__FAKER__: Faker = None
__JINJA__ENV__: Environment = None


def convert_type_name(source_type: str) -> str:
    """
    Jinja filter to convert model type to spark type
    """
    return convert_model_type_to_spark_type(source_type).__name__


def split(value: str, pattern: str = ".") -> list:
    """
    Jinja filter to split a string and return a list
    """
    return value.split(pattern)


def split_and_pick(value, idx: int) -> str:
    """
    Jinja filter to split a string and take the n-th element identified by idx
    """
    return value.rsplit(".")[idx]


def is_nullable_column(column: ConfigContainer) -> bool:
    """
    Return True if the column is nullable
    """
    return not (hasattr(column, "constraints") and "non_null" in [c.type for c in column.constraints])


def strformat(value, frmt_str):
    """
    Jinja filter for formatting a string
    """
    return frmt_str % value


def tuple_mapper(key_value: tuple, split_str: str = "=") -> str:
    return f"{key_value[0]}{split_str}{key_value[1]}"


def faker(column: ConfigContainer):
    global __FAKER__
    if not __FAKER__:
        __FAKER__ = Faker()
        #  __FAKER__.sed(10)

    col_type = convert_model_type_to_spark_type(column.type)
    if col_type == StringType:
        processed_id = column.id.replace("_", " ").replace("-", " ").lower()
        return get_text_fakers(processed_id, __FAKER__)
    elif col_type == BooleanType:
        return random.choice([True, False])
    elif col_type in [DecimalType, DoubleType, FloatType, IntegerType, LongType, NumericType, ShortType, DateType]:
        return get_fake_value_by_spark_type(col_type.__name__, __FAKER__)
    elif col_type in [TimestampType]:
        return time.time()
    else:
        return "N/A"


def init():
    global __JINJA__ENV__
    __JINJA__ENV__ = Environment(
        lstrip_blocks=True, trim_blocks=True, loader=FileSystemLoader(os.path.join(Path(__file__).parent, "templates"))
    )
    __JINJA__ENV__.filters["convert_type_name"] = convert_type_name
    __JINJA__ENV__.filters["is_nullable"] = is_nullable_column
    __JINJA__ENV__.filters["strformat"] = strformat
    __JINJA__ENV__.filters["tuple_mapper"] = tuple_mapper
    __JINJA__ENV__.filters["faker"] = faker
    __JINJA__ENV__.filters["split_and_pick"] = split_and_pick
    __JINJA__ENV__.filters["dataset_id"] = resolve_data_set_id
    __JINJA__ENV__.filters["product_id"] = resolve_product_id
    __JINJA__ENV__.filters["model_id"] = resolve_model_id
    __JINJA__ENV__.filters["split"] = split


def get_jinja_env() -> Environment:
    if not __JINJA__ENV__:
        init()
    return __JINJA__ENV__
    #  else:
    #      raise RuntimeError("The Jinja Environment is not initialized yet. Call init() first.")


def generate_pytest_ini():
    pytest_ini_template = get_jinja_env().get_template("pytest.ini.j2")
    return pytest_ini_template.render()


def generate_task_logic(
    inputs: list[ConfigContainer] | None = None,
    outputs: list[ConfigContainer] | None = None,
    param_list: list[ConfigContainer] | None = None,
) -> str:
    task_template = get_jinja_env().get_template("task_logic.py.j2")
    input_ids = [resolve_data_set_id(io) for io in inputs] if inputs else []
    output_ids = [resolve_data_set_id(io) for io in outputs] if outputs else []
    if param_list is not None:
        param_list = param_list.__dict__.items()
    return task_template.render(inputs=input_ids, outputs=output_ids, params=param_list)


def generate_task_test_logic(
    task_name: str,
    inputs: list[ConfigContainer] | None = None,
    outputs: list[ConfigContainer] | None = None,
    params: list[ConfigContainer] | None = None,
    models: list[ConfigContainer] | None = None,
) -> str:
    task_test_template = get_jinja_env().get_template("test_task_logic.py.j2")
    input_ids = [resolve_data_set_id(io) for io in inputs] if inputs else []
    output_ids = [resolve_data_set_id(io) for io in outputs] if outputs else []
    if params is not None:
        params = params.__dict__.items()
    return task_test_template.render(
        task_name=task_name, inputs=input_ids, outputs=output_ids, params=params, models=models
    )


def generate_fixtures(model_definition: ConfigContainer | None = None, input_ids: list[str] | None = None) -> str:
    fixture_template = get_jinja_env().get_template("test_config.py.j2")
    return fixture_template.render(models=model_definition.models, input_ids=input_ids)
