import os
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from driver.util import convert_model_type_to_spark_type
from driver.core import ConfigContainer, resolve_data_set_id

#  from pyspark.sql.types import DataType

__JINJA__ENV__: Environment = None


def convert_type_name(source_type: str) -> str:
    return convert_model_type_to_spark_type(source_type).__name__


def is_nullable_column(column: ConfigContainer) -> bool:
    """
    Return True if the column is nullable
    """
    return not (hasattr(column, "constraints") and "non_null" in [c.type for c in column.constraints])


def strformat(value, frmt_str):
    return frmt_str % value


def init():
    global __JINJA__ENV__
    __JINJA__ENV__ = Environment(
        lstrip_blocks=True, trim_blocks=True, loader=FileSystemLoader(os.path.join(Path(__file__).parent, "templates"))
    )
    __JINJA__ENV__.filters["convert_type_name"] = convert_type_name
    __JINJA__ENV__.filters["is_nullable"] = is_nullable_column
    __JINJA__ENV__.filters["strformat"] = strformat


def get_jinja_env() -> Environment:
    if not __JINJA__ENV__:
        init()
    return __JINJA__ENV__
    #  else:
    #      raise RuntimeError("The Jinja Environment is not initialized yet. Call init() first.")


def generate_task_logic(
    inputs: list[ConfigContainer] | None = None,
    outputs: list[ConfigContainer] | None = None,
    params: list[ConfigContainer] | None = None,
) -> str:
    task_template = get_jinja_env().get_template("task_logic.py.j2")
    input_ids = [resolve_data_set_id(io) for io in inputs] if inputs else []
    output_ids = [resolve_data_set_id(io) for io in outputs] if outputs else []
    if params is not None:
        params = params.__dict__.keys()
    return task_template.render(inputs=input_ids, outputs=output_ids, params=params)


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
        params = params.__dict__.keys()
    print(f"-> {task_name} / {input_ids} / {output_ids}")
    return task_test_template.render(
        task_name=task_name, inputs=input_ids, outputs=output_ids, params=params, models=models
    )


def generate_fixtures(model_definition: ConfigContainer | None = None, input_ids: list[str] | None = None) -> str:
    fixture_template = get_jinja_env().get_template("test_config.py.j2")
    return fixture_template.render(models=model_definition.models, input_ids=input_ids)
