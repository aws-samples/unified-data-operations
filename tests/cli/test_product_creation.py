import os

from pyspark.sql.types import StructField, StructType, IntegerType, StringType
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from pytest import fixture

from cli.data_product import generate_task_logic
from driver.core import ConfigContainer
from driver.util import label_io_types_on_product


def df_schema():
    return StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("last_name", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
        ]
    )


@fixture(scope="module")
def product_defition() -> ConfigContainer:
    product_definition_dict = {
        "schema": "1.2.3_rc1",
        "product": {
            "id": "test_product",
            "version": "1.0.0",
            "name": "test product",
            "description": "Product Description",
            "owner": "john@doe.com",
            "pipeline": {
                "schedule": "* 1 0 * 1",
                "tasks": [
                    {
                        "id": "ingest",
                        "logic": {"module": "tasks.ingest", "parameters": {"timestamp": "True"}},
                        "inputs": [{"connection": "grocery", "table": "sales.something"}],
                        "outputs": [{"model": "datalake.something_else"}],
                    }
                ],
            },
        },
    }
    product_definition = ConfigContainer.create_from_dict(product_definition_dict)
    label_io_types_on_product(product_definition)
    return product_definition


@fixture(scope="module")
def jinja_env() -> Environment:
    return Environment(loader=FileSystemLoader(os.path.join(Path(__file__).parent.parent, "cli", "templates")))


def tast_test():
    pass


def test_task_temaplate(product_defition, jinja_env):
    #  task_template = jinja_environment.get_template('task_logic.py.j2')
    #  input_ids =
    #  template = task_template.render(inputs=input_ids, outputs=output_ids, params=params)
    task = product_defition.product.pipeline.tasks[0]
    content = generate_task_logic(inputs=task.inputs, outputs=task.outputs, param_list=task.logic.parameters)
    print(content)
    assert content
