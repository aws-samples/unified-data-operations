import os
import re
import traceback
import driver
import click
import yaml
import driver.aws.providers
from pathlib import Path
from typing import Iterable, List, Dict
from prompt_toolkit import HTML, print_formatted_text, prompt
from prompt_toolkit.completion import (
    CompleteEvent,
    Completer,
    Completion,
    WordCompleter,
)
from driver import task_executor
from prompt_toolkit.document import Document
from prompt_toolkit.shortcuts import CompleteStyle
from prompt_toolkit.validation import Validator
from cli.common import (
    aws,
    collect_bool,
    collect_key_value_pairs,
    cron_expression_prompt,
    driver_subsystem,
    non_empty_prompt,
    style,
)
from driver.core import ConfigContainer, resolve_data_set_id
from .bindings import kb
from .jinja import generate_task_logic, generate_fixtures, generate_task_test_logic
from driver.util import create_model_from_spark_schema
import pickle
import json

# todo: remove below imports
# from prompt_toolkit.application import get_app
# from prompt_toolkit.document import Document
# from prompt_toolkit.shortcuts import CompleteStyle, print_container
# from prompt_toolkit.validation import Validator
# import driver.aws.providers
# from cli.data_product import get_io_type

io_types = ["connection", "model", "file"]
product_temp_file_name = ".product.dict"
model_temp_file_name = ".model.dict"


#  class CronCompleter(Completer):
#      def __init__(self) -> None:
#          super().__init__()

#      def get_completions(self, document: Document, complete_event: CompleteEvent) -> Iterable[Completion]:
#          pass


class IOCompleter(Completer):
    def __init__(self) -> None:
        super().__init__()
        self.tables = None
        self.glue = driver.aws.providers.get_glue()
        self.connections = self.glue.get_connections(HidePassword=True).get("ConnectionList")
        # todo: iterate over all results
        self.databases = self.glue.get_databases(ResourceShareType="ALL", MaxResults=250).get("DatabaseList")

    def get_completions(self, document: Document, complete_event: CompleteEvent) -> Iterable[Completion]:
        try:
            colon_pattern = re.compile(r"^([^:(?\s)]+)")
            dot_pattern = re.compile(r"^([^.]+)")

            def handle_data_catalog(use_connections: bool = True):
                offset = 1 if use_connections else 0
                if words_cnt == offset:
                    connections = [cid.get("Name") for cid in self.connections]
                    connections_completer = WordCompleter(connections, pattern=colon_pattern)
                    yield from connections_completer.get_completions(document, complete_event)
                elif words_cnt == 1 + offset:
                    datase_names = [db.get("Name") for db in self.databases]
                    db_completer = WordCompleter(
                        datase_names, pattern=dot_pattern if use_connections else colon_pattern
                    )
                    yield from db_completer.get_completions(document, complete_event)
                elif words_cnt >= 2 + offset:
                    try:
                        database_name = (
                            ".".join(fragments[1 : len(fragments) - 1]) if len(fragments[1:]) > 2 else fragments[offset]
                        )
                        tables = self.glue.get_tables(DatabaseName=database_name).get("TableList")
                        table_completer = WordCompleter([tr.get("Name") for tr in tables], pattern=dot_pattern)
                        yield from table_completer.get_completions(document, complete_event)
                    except Exception:
                        pass

            prefix = document.text.split(":", 1)
            fragments = [f.strip() for f in prefix[1].split(".")] if len(prefix) > 1 else []
            words_cnt = len(fragments)
            io_type_meta = {
                "connection": "{connection name}.{schema name}.{table name}",
                "model": "{database name}.{table name}",
                "file": "{s3 path}",
            }
            if len(prefix) == 0 or prefix[0] not in io_types:
                prefix_completer = WordCompleter(io_types, meta_dict=io_type_meta, pattern=colon_pattern)
                #  prefix_completer = WordCompleter(io_types, meta_dict=io_type_meta)
                yield from prefix_completer.get_completions(document, complete_event)
            elif prefix[0] == "connection":
                #  get_app().current_buffer.insert_text(': ')
                yield from handle_data_catalog()
            elif prefix[0] == "model":
                yield from handle_data_catalog(use_connections=False)
            elif prefix[0] == "file":
                pass
            else:
                prefix_completer = WordCompleter(io_types, meta_dict=io_type_meta)
                yield from prefix_completer.get_completions(document, complete_event)
        except Exception:
            pass


def get_io_type(prompt_text: str):
    def validate_io(text):
        tokens = text.split(":", 1)
        valid = len(tokens) > 1 and tokens[0] in io_types
        if not valid:
            return valid
        elif tokens[0] == "connection":
            return len(text.split(":", 1)[1].split(".")) >= 3
        elif tokens[0] == "model":
            return len(text.split(":", 1)[1].split(".")) >= 2
        elif tokens[0] == "file":
            return True

    def parse_url(io_url):
        fragments = io_url.split(":", 1)
        if fragments[0] == "connection":
            connection_parts = fragments[1].split(".", 1)
            return {"connection": connection_parts[0].strip("' "), "table": connection_parts[1]}
        elif fragments[0] == "model":
            return {"model": fragments[1].strip("' ")}
        elif fragments[0] == "file":
            return {"file": fragments[1].strip("' '")}
        else:
            return io_url

    io_validator = Validator.from_callable(
        validate_io, error_message="Required pattern: type: [connection].schema.table", move_cursor_to_end=True
    )
    return parse_url(
        prompt(
            prompt_text,
            completer=IOCompleter(),
            key_bindings=kb,
            validator=io_validator,
            complete_style=CompleteStyle.MULTI_COLUMN,
            complete_while_typing=True,
        )
    )


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
                f"Connecting to: <green>{resolve_data_set_id(input_definition)}</green> "
                f"using input definition {input_definition.to_dict()} with identified input "
                f"handler: {handle_input.__name__ if handle_input else None}"
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


def re_define_model(model: dict):
    #  print_formatted_text(HTML(f"<green>{model}</green>"), style=style)
    #  def create_edit_column
    # todo: add column management code abive /
    # support: 3 cases: columns are empty, columns need redefinition, new columns should be added
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
                col.update(id=non_empty_prompt("Id: ", col.get("id")))
                # todo: add type mapper
                col.update(type=non_empty_prompt("Type: ", col.get("type")))
                col.update(name=non_empty_prompt("Name: ", replace_multiple(col.get("name", "")).title()))
                col.update(description=non_empty_prompt("Description: ", col.get("description", "").capitalize()))
                # todo: add constraints support
                #
                cols[idx] = col
            model.update(columns=cols)
        elif not cols and collect_bool("Do you want to add columns?", False):
            pass
        model = (
            model
            | {"meta": collect_key_value_pairs("Do you want to add metadata?", "Meta Data")}
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
            model_list[idx] = re_define_model(model)
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
            task_content = generate_task_logic(inputs=task.inputs, outputs=task.outputs, params=task.logic.parameters)
            write_file(os.path.join(product_folder, "tasks", f"{task_name}.py"), task_content)
            fixture_content = generate_fixtures(model_definition)
            write_file(os.path.join(product_folder, "tests", f"test_config.py"), fixture_content)
            task_test_content = generate_task_test_logic(
                inputs=task.inputs, outputs=task.outputs, params=task.logic.parameters, models=model_definition.models
            )
            write_file(os.path.join(product_folder, "tests", f"test_{task_name}.py"), task_test_content)


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
            "Temporary product definition file is found. Do you want to continue it from here? [Y/N]", True
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
            "Temporary models definition files is found. Do you want to continue it from here? [Y/N]", True
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
