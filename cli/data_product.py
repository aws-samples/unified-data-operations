import os
import re
import traceback

from pandas.tests.internals.test_internals import N
import driver
import click
import yaml
import driver.aws.providers
from pathlib import Path
from typing import List, Dict
from prompt_toolkit import HTML, print_formatted_text, prompt
from prompt_toolkit.completion import (
    WordCompleter,
)
from driver import task_executor
from cli.common import (
    aws,
    collect_bool,
    collect_key_value_pairs,
    cron_expression_prompt,
    driver_subsystem,
    non_empty_prompt,
    style,
    select_from_list,
)
from driver.core import ConfigContainer, resolve_data_set_id
from .jinja import generate_task_logic, generate_fixtures, generate_task_test_logic, generate_pytest_ini
from .common import get_io_type
from driver.util import create_model_from_spark_schema
import pickle
import json

product_temp_file_name = ".product.dict"
model_temp_file_name = ".model.dict"


#  class CronCompleter(Completer):
#      def __init__(self) -> None:
#          super().__init__()

#      def get_completions(self, document: Document, complete_event: CompleteEvent) -> Iterable[Completion]:
#          pass


def get_schema_definitions():
    container_folder = Path(__file__).parent.parent
    schema_folder = Path(os.path.join(container_folder, "driver", "schema"))
    supported_schemas = [x.name for x in schema_folder.iterdir() if x.is_dir()]
    supported_schemas.sort()
    completer = WordCompleter(words=supported_schemas)
    schema = prompt(
        "Data Product Schema: ", completer=completer, complete_while_typing=True, default=supported_schemas[0]
    )
    return schema


def get_pipeline():
    def get_logic(default_task_id: str):
        if not collect_bool("Do you want to add a custom logic? "):
            return None
        return {
            "module": non_empty_prompt("Provide module name: ", default=f"tasks.{default_task_id}"),
            "parameters": collect_key_value_pairs(question="Do you want to add params?", key_name="Parameter"),
        }

    def get_ios(prefix: str):
        ios = list()
        while True:
            ios.append(get_io_type(prefix))
            if not collect_bool("Add another one? "):
                break
        return ios

    def get_task():
        task_id = non_empty_prompt("Task ID: ")
        task_dict = {"id": task_id, "inputs": get_ios("Input: "), "outputs": get_ios("Output: ")}
        logic = get_logic(task_id)
        if logic:
            task_dict.update(logic=logic)
        return task_dict

    trigger = cron_expression_prompt("Schedule crontab: ")
    tasks = list()
    while True:
        tasks.append(get_task())
        if not collect_bool("Add another one task? "):
            break
    # todo: add cron job validation and add upstream dependency
    return {"schedule": trigger, "tasks": tasks}


def collect_data_product_definition(name: str) -> ConfigContainer:
    product_name = non_empty_prompt("Product name: ", default=name)
    product_defintion = ConfigContainer.create_from_dict(
        {
            "schema_version": get_schema_definitions(),
            "product": {
                "id": non_empty_prompt("Unique data product ID: ", default=product_name.replace(" ", "_")),
                "version": non_empty_prompt("Version: ", default="1.0.0"),
                "name": product_name,
                "description": non_empty_prompt("Description: "),
                "owner": non_empty_prompt("Owner e-mail: "),
                "pipeline": get_pipeline(),
            },
        }
    )
    driver.util.label_io_types_on_product(product_defintion)
    return product_defintion


def collect_schema_from_data_source(input_definition: ConfigContainer):
    model = None
    try:
        handle_input = task_executor.data_src_handlers.get(input_definition.type)
        print_formatted_text(
            HTML(
                f"Connecting to: <green>{resolve_data_set_id(input_definition)}</green>\n"
                f"  using input definition <path>{input_definition.to_dict()}</path>\n"
                f"  with identified input handler: <levender>{handle_input.__name__ if handle_input else None}</levender>"
            ),
            style=style,
        )
        #  df = io_handlers.connection_input_handler(props=connection_config)
        if not handle_input:
            raise Exception(f"No input handler is defined for {input_definition.type}")
        df = handle_input(input_definition)
        model_id = resolve_data_set_id(input_definition)
        model = create_model_from_spark_schema(df, model_id, "1", model_id, model_id)
    except Exception as ex:
        print_formatted_text(
            HTML(
                f"<red>{str(type(ex).__name__)} exception caught</red> "
                f"while collecting schema information. Skipping schema detection. "
                f"Complete Exception message: \n{str(ex)}"
            ),
            style=style,
        )
        traceback.print_exc()
    finally:
        return model


def re_define_model(model: dict, other_models: list = []):
    # todo: add column management code above /
    # support: 3 cases: columns are empty, columns need redefinition, new columns should be added
    def configure_column(existing: dict = None):
        col = {
            "id": non_empty_prompt("Id: ", existing.get("id") if existing else None),
            "type": non_empty_prompt("Type: ", existing.get("type") if existing else None),
            "name": non_empty_prompt(
                "Name: ", replace_multiple(existing.get("name", "")).title() if existing else None
            ),
            "description": non_empty_prompt(
                "Description: ", existing.get("description", "").capitalize() if existing else None
            ),
        }
        # todo: add constraints support
        return col

    print(f"Using model:\n{json.dumps(model, indent=4)}")
    if collect_bool(f"Do you want to edit Model {model.get('id')}?", False):
        replacers = {"-": " ", "_": " "}

        def replace_multiple(original: str):
            for k, v in replacers.items():
                original = original.replace(k, v)
            return original

        model.update(id=non_empty_prompt("Model ID: ", model.get("id")))
        model.update(version=non_empty_prompt("Version: ", model.get("version")))
        model.update(name=non_empty_prompt("Name: ", replace_multiple(model.get("name", "")).title()))
        model.update(description=non_empty_prompt("Description: ", replace_multiple(model.get("description", ""))))
        cols = model.get("columns", [])
        if cols and collect_bool("Do you want to edit the columns? ", False):
            for idx, col in enumerate(cols):
                cols[idx] = configure_column(col)
            model.update(columns=cols)
        elif not cols and collect_bool("Do you want to add columns?", False):
            if other_models and collect_bool("Do you want to copy fields from another model?", False):
                other_model_id = select_from_list(
                    "Select model to copy from: ", [other_model.get("id") for other_model in other_models]
                )
                other_model = next(iter([m for m in other_models if m.get("id") == other_model_id]), None)
                if other_model:
                    model.update(columns=other_model.get("columns"))
            else:
                cols: List[Dict] = list()
                while True:
                    cols.append(configure_column())
                    if not collect_bool("Add another one? "):
                        break
                model.update(columns=cols)
        model = (
            model
            | {"meta": model.get("meta", {}) | collect_key_value_pairs("Do you want to add metadata?", "Meta Data")}
            | {"tags": collect_key_value_pairs("Do you want to add cost allocation tags?", "Cost Allocation Tag")}
            | {"access": collect_key_value_pairs("Do you want to add access labels?", "Access Tag")}
        )
        # todo: add storage support
    return model


def collect_model_definition(data_product: ConfigContainer):
    input_models = list()
    output_models = list()

    def augment_models(model_list: List[Dict]) -> List[Dict]:
        for idx, model in enumerate(model_list):
            model_list[idx] = re_define_model(model, model_list)
        return model_list

    for task in data_product.product.pipeline.tasks:
        if hasattr(task, "inputs"):
            for input_def in task.inputs:
                detected_model = collect_schema_from_data_source(input_def)
                if detected_model is not None:
                    input_models.append(detected_model)
        input_ids = set([inp_model.get("id") for inp_model in input_models])
        if hasattr(task, "outputs"):
            expr = re.compile("[_|-]")
            for output_def in task.outputs:
                output_model_id = resolve_data_set_id(output_def)
                if output_model_id not in input_ids:
                    output_models.append(
                        {
                            "id": output_model_id,
                            "version": "1",
                            "name": expr.sub(" ", output_model_id),
                            "description": "",
                        }
                    )
    # todo: check the status of medata extracted via Spark
    return ConfigContainer.create_from_dict(
        {"schema_version": data_product.schema_version, "models": augment_models(input_models + output_models)}
    )


def generate_product(product_def: ConfigContainer, model_definition: ConfigContainer):
    def dump_dict_to_yaml(dict_obj: dict, full_path: str):
        with open(full_path, "w") as dump_file:
            yaml.dump(dict_obj, dump_file, sort_keys=False, default_flow_style=False)

    def write_file(path: str, content: str):
        with open(path, "w") as file:
            file.write(content)

    def create_folders(prefix: str, folder_paths: list[str] | list[list[str]]):
        for folder in folder_paths:
            if isinstance(folder, list):
                os.makedirs(os.path.join(prefix, *folder))
            else:
                os.makedirs(os.path.join(prefix, folder))

    product_folder_name = product_def.product.id.replace(" ", "_")
    product_folder = os.path.join(os.getcwd(), product_folder_name)
    if os.path.exists(product_folder):
        raise FileExistsError(f"Product folder [{product_folder_name}] already exists.")
    create_folders(product_folder, folder_paths=["tasks", "tests"])
    dump_dict_to_yaml(product_def.to_dict(), os.path.join(product_folder, "product.yml"))
    dump_dict_to_yaml(model_definition.to_dict(), os.path.join(product_folder, "model.yml"))
    for task in product_def.product.pipeline.tasks:
        if task.logic is not None:
            task_name = task.logic.module.split(".")[-1]
            task_content = generate_task_logic(
                inputs=task.inputs, outputs=task.outputs, param_list=task.logic.parameters
            )
            write_file(os.path.join(product_folder, "tasks", f"{task_name}.py"), task_content)
            fixture_content = generate_fixtures(
                model_definition, [resolve_data_set_id(inp_def) for inp_def in task.inputs]
            )
            write_file(os.path.join(product_folder, "tests", "test_config.py"), fixture_content)
            task_test_content = generate_task_test_logic(
                task_name=task_name,
                inputs=task.inputs,
                outputs=task.outputs,
                params=task.logic.parameters,
                models=model_definition.models,
            )
            write_file(os.path.join(product_folder, "tests", f"test_{task_name}.py"), task_test_content)
    pytest_ini_content = generate_pytest_ini()
    write_file(os.path.join(product_folder, "pytest.ini"), pytest_ini_content)


@click.command(name="create")
@click.option("-n", "--name", required=True, type=str, help="Name of the data product")
@aws
@driver_subsystem
def create_data_product(name):
    def cleanup():
        if os.path.exists(product_temp_file_name):
            os.remove(product_temp_file_name)
        if os.path.exists(model_temp_file_name):
            os.remove(model_temp_file_name)

    try:
        #      app = get_app()
        #      b = app.current_buffer
        #      if b.complete_state:
        #          b.complete_next()
        #      else:
        #          b.start_completion(select_first=False)
        #  b.insert_text(" ")
        if os.path.exists(product_temp_file_name) and collect_bool(
            "Temporary product definition file is found. Do you want to continue it from here?", True
        ):
            with open(product_temp_file_name, "rb") as pp:
                dp_definition = pickle.load(pp)
                #  print_formatted_text(
                #      HTML(f"Using product definition:<br> <green>{str(dp_definition)}</green>"),
                #      style=style,
                #  )
                print(f"Using product definition:\n {json.dumps(dp_definition.to_dict(), indent=4)}")
        else:
            dp_definition = collect_data_product_definition(name=name)
            with open(product_temp_file_name, "wb") as pp:
                pickle.dump(dp_definition, pp)
        if os.path.exists(model_temp_file_name) and collect_bool(
            "Temporary models definition files is found. Do you want to continue it from here?", True
        ):
            with open(model_temp_file_name, "rb") as mp:
                models_definition = pickle.load(mp)
                print(f"Using models definition: \n{json.dumps(models_definition.to_dict(), indent=4)}")
        else:
            models_definition = collect_model_definition(dp_definition)
            with open(model_temp_file_name, "wb") as mp:
                pickle.dump(models_definition, mp)
            dp = dp_definition.product
            print_formatted_text(
                HTML(f"Product name: <green>{dp.name}</green> and ID:  <green>{dp.id}</green>"),
                style=style,
            )
        generate_product(dp_definition, models_definition)
        #  print_formatted_text(HTML(f"Input: <green>{ds_input}</green>"), style=style)
        cleanup()
    except Exception as ex:
        traceback.print_exc()
        print_formatted_text(
            HTML(f"<red>{type(ex).__name__} error caught, with message: </red> {str(ex)}"),
            style=style,
        )
