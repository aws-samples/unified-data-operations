import io
from pydoc import resolve
import traceback
import re
import os
import click
import yaml
import driver
from driver.core import ConfigContainer, resolve_data_set_id
from driver.util import parse_dict_into_object
from jinja2 import Environment, FileSystemLoader, environment
from pathlib import Path
from prompt_toolkit import prompt
from prompt_toolkit.completion import Completer, WordCompleter, Completion, CompleteEvent
from prompt_toolkit.document import Document
from prompt_toolkit.shortcuts import CompleteStyle
from prompt_toolkit.validation import Validator
from typing import Iterable
from .bindings import kb
from cli.common import collect_key_value_pairs, non_empty_prompt, style, aws, collect_bool, collect_key_value_pairs, validated_prompt
from prompt_toolkit import HTML, print_formatted_text
#  from prompt_toolkit.application import get_app
#  from prompt_toolkit.document import Document
#  from prompt_toolkit.shortcuts import CompleteStyle, print_container
#  from prompt_toolkit.validation import Validator
#  import driver.aws.providers
#  from cli.data_product import get_io_type

io_types = ['connection', 'model', 'file']
jinja_environment = Environment(loader=FileSystemLoader(os.path.join(Path(__file__).parent, 'templates')))

class IOCompleter(Completer):
    def __init__(self)-> None:
        super().__init__()
        self.tables = None
        self.glue = driver.aws.providers.get_glue()
        self.connections = self.glue.get_connections(HidePassword=True).get('ConnectionList')
        self.databases = self.glue.get_databases(ResourceShareType='ALL').get('DatabaseList')

    def get_completions(self, document: Document, complete_event: CompleteEvent) -> Iterable[Completion]:
        try:
            colon_pattern = re.compile(r'^([^:(?\s)]+)')
            dot_pattern = re.compile(r'^([^.]+)')
            def handle_data_catalog(use_connections: bool = True):
                offset = 1 if use_connections else 0
                if words_cnt == offset:
                    connections = [cid.get('Name') for cid in self.connections]
                    connections_completer = WordCompleter(connections, pattern=colon_pattern)
                    yield from connections_completer.get_completions(document, complete_event)
                elif words_cnt == 1+offset:
                    datase_names = [db.get('Name') for db in self.databases]
                    db_completer = WordCompleter(datase_names, pattern=dot_pattern if use_connections else colon_pattern)
                    yield from db_completer.get_completions(document, complete_event)
                elif words_cnt >= 2+offset:
                    try:
                        database_name = '.'.join(fragments[1:len(fragments)-1]) if len(fragments[1:])>2 else fragments[offset]
                        tables = self.glue.get_tables(DatabaseName=database_name).get('TableList')
                        table_completer = WordCompleter([tr.get('Name') for tr in tables], pattern=dot_pattern)
                        yield from table_completer.get_completions(document, complete_event)
                    except Exception:
                        pass

            prefix = document.text.split(':', 1)
            fragments = [f.strip() for f in prefix[1].split('.')] if len(prefix)>1 else []
            words_cnt = len(fragments)
            io_type_meta = {
                    'connection': '{connection name}.{schema name}.{table name}',
                    'model': '{database name}.{table name}',
                    'file': '{s3 path}'
                    }
            if len(prefix) == 0 or prefix[0] not in io_types:
                prefix_completer = WordCompleter(io_types, meta_dict=io_type_meta, pattern=colon_pattern)
                #  prefix_completer = WordCompleter(io_types, meta_dict=io_type_meta)
                yield from prefix_completer.get_completions(document, complete_event)
            elif prefix[0] == 'connection':
                #  get_app().current_buffer.insert_text(': ')
                yield from handle_data_catalog()
            elif prefix[0] == 'model':
                yield from handle_data_catalog(use_connections=False)
            elif prefix[0] == 'file':
                pass
            else:
                prefix_completer = WordCompleter(io_types, meta_dict=io_type_meta)
                yield from prefix_completer.get_completions(document, complete_event)
        except Exception:
            pass

def get_io_type(prompt_text: str):
    def validate_io(text):
        tokens = text.split(':',1)
        valid = len(tokens)>1 and tokens[0] in io_types
        if not valid:
            return valid
        elif tokens[0] == 'connection':
            return len(text.split(':',1)[1].split('.'))>=3
        elif tokens[0] == 'model':
            return len(text.split(':',1)[1].split('.'))>=2
        elif tokens[0] == 'file':
            return True
    def parse_url(io_url):
        fragments = io_url.split(':',1)
        if fragments[0] == 'connection':
            connection_parts = fragments[1].split('.')
            return {
                    'connection': connection_parts[0].strip("' "),
                    'table': connection_parts[1]
                    }
        elif fragments[0] == 'model':
            return {'model': fragments[1].strip("' ")}
        elif fragments[0] == 'file':
            return {'file': fragments[1].strip("' '")}
        else: return io_url

    io_validator = Validator.from_callable(validate_io, error_message = 'Required pattern: type: [connection].schema.table', move_cursor_to_end=True)
    return parse_url(prompt(prompt_text, completer=IOCompleter(),
                      key_bindings=kb,
                      validator=io_validator,
                      complete_style=CompleteStyle.COLUMN,
                      complete_while_typing=True))

def get_schema():
    container_folder = Path(__file__).parent.parent
    schema_folder = Path(os.path.join(container_folder, 'driver','schema'))
    supported_schemas = [x.name for x in schema_folder.iterdir() if x.is_dir()]
    supported_schemas.sort()
    completer = WordCompleter(words=supported_schemas)
    schema = prompt('Data Product Schema: ',
                    completer=completer,
                    complete_while_typing=True,
                    default=supported_schemas[0])
    return schema


def get_pipeline():
    def get_logic(default_task_id: str):
        if not collect_bool('Do you want to add a custom logic? '):
            return None
        return {
                'module': non_empty_prompt('Provide module name: ', default=f'tasks.{default_task_id}'),
                'parameters': collect_key_value_pairs(question='Do you want to add params?', key_name='parameter')
                }
    def get_ios(prefix: str):
        ios = list()
        while True:
            ios.append(get_io_type(prefix))
            if not collect_bool('Add another one? '):
                break
        return ios
    def get_task():
        task_id = non_empty_prompt('Task ID: ')
        task_dict = {
            'id': task_id,
            'inputs': get_ios('Input: '),
            'outputs': get_ios('Output: ')
            }
        logic = get_logic(task_id)
        if logic:
            task_dict.update(logic = logic)
        return task_dict
    trigger = non_empty_prompt('Trigger: ')
    tasks = list()
    while True:
        tasks.append(get_task())
        if not collect_bool('Add another one task? '):
            break
    #todo: add cron job validation and add upstream dependency
    return {
            'schedule': trigger,
            'tasks': tasks
            }

def collect_data_product_definition(name: str):
        product_name = non_empty_prompt('Product name: ', default=name)
        return {
                'schema': get_schema(),
                'product': {
                    'id': non_empty_prompt("Unique data product ID: ", default=product_name.replace(' ', '_')),
                    'version': non_empty_prompt('Version: ', default='1.0.0'),
                    'name': product_name,
                    'description': non_empty_prompt('Description: '),
                    'owner': non_empty_prompt('Owner e-mail: '),
                    'pipeline': get_pipeline(),
                    }
                }

def collect_model_definition():
    return {}

def generate_task_logic(
                        inputs: list[ConfigContainer]|None = None,
                        outputs: list[ConfigContainer]|None = None,
                        params: list[ConfigContainer]|None = None):
    task_template = jinja_environment.get_template('task_logic.py.j2')
    def xtract_io_name(io_entry: ConfigContainer):
        io_type: str = list(set(dir(io_entry)) & set(io_types))[0]
        extractors = {
                'connection': lambda ioe: ioe.table.split('.')[-1],
                'model': lambda ioe: ioe.model.split('.')[-1],
                'file': lambda ioe: ioe.file.split('/')[-1]
                }
        return extractors.get(io_type)(io_entry)
    input_ids = [xtract_io_name(io) for io in inputs]
    output_ids = [xtract_io_name(io) for io in outputs]
    if params is not None:
        params = params.__dict__.keys()
    return task_template.render(inputs=input_ids, outputs=output_ids, params=params)

def generate_product(product_dict: dict, models_dict: dict):
    product = parse_dict_into_object(product_dict).product
    models = parse_dict_into_object(models_dict)
    product_folder = os.path.join(os.getcwd(), product.id.replace(' ', '_'))
    os.makedirs(os.path.join(product_folder, 'tasks'))
    os.makedirs(os.path.join(product_folder, 'tests'))
    with open(os.path.join(product_folder, 'product.yml'), 'w') as outfile:
        yaml.dump(product_dict, outfile, sort_keys=False, default_flow_style=False)
    for task in product.pipeline.tasks:
        if task.logic is not None:
            print(task)
            content = generate_task_logic(inputs=task.inputs,
                                          outputs=task.outputs,
                                          params=task.logic.parameters)
            module_file_name = f"{task.logic.module.split('.')[-1]}.py"
            with open(os.path.join(product_folder, 'tasks', module_file_name), 'w') as outfile:
                outfile.write(content)

@click.command(name="create")
@click.option("-n", "--name", required=True, type=str, help='Name of the data product')
@aws
def create_data_product(name):
    try:
        #      app = get_app()
        #      b = app.current_buffer
        #      if b.complete_state:
        #          b.complete_next()
        #      else:
        #          b.start_completion(select_first=False)
           #  b.insert_text(" ")
        dp_definition = collect_data_product_definition(name=name)
        models_definition = collect_model_definition()
        dp = dp_definition.get('product')
        print_formatted_text(
            HTML(f"Product name: <green>{dp.get('name')}</green> and ID:  <green>{dp.get('id')}</green>"),
            style=style
        )
        generate_product(dp_definition, models_definition)
        #  print_formatted_text(HTML(f"Input: <green>{ds_input}</green>"), style=style)
    except Exception as ex:
        traceback.print_exc()
        print(ex)

