import traceback
import re
from pathlib import Path
from prompt_toolkit import prompt
import prompt_toolkit
from prompt_toolkit.completion import Completer, WordCompleter, Completion, CompleteEvent
from prompt_toolkit.document import Document
from prompt_toolkit.shortcuts import CompleteStyle
from prompt_toolkit.validation import Validator
from typing import Iterable
from .bindings import kb
import driver.aws.providers
import click
from cli.common import non_empty_prompt, style, aws, collect_bool, collect_key_value_pair
from prompt_toolkit import HTML, print_formatted_text
#  from prompt_toolkit.application import get_app
#  from prompt_toolkit.document import Document
#  from prompt_toolkit.shortcuts import CompleteStyle, print_container
#  from prompt_toolkit.validation import Validator
#  import driver.aws.providers
#  from cli.data_product import get_io_type

io_types = ['connection', 'model', 'file']

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

    io_validator = Validator.from_callable(validate_io, error_message = 'Required pattern: type: [connection].schema.table', move_cursor_to_end=True)
    return prompt(prompt_text, completer=IOCompleter(),
                      key_bindings=kb,
                      validator=io_validator,
                      complete_style=CompleteStyle.COLUMN,
                      complete_while_typing=True)

def get_schema():
    container_folder = Path(__file__).parent.parent
    schema_folder = Path(f'{container_folder}/driver/schema')
    #  current_dir = Path('.').parent()
    supported_schemas = [x.name for x in schema_folder.iterdir() if x.is_dir()]
    supported_schemas.sort()
    completer = WordCompleter(words=supported_schemas)
    schema = prompt('Data Product Schema: ',
                    completer=completer,
                    complete_while_typing=True,
                    default=supported_schemas[0])
    return schema


def get_pipeline():
    def get_logic():
        if not collect_bool('Do you want to add a custom logic? '):
            return None
        module_name = non_empty_prompt('Gimme stuff: ', default='tasks.')
        params = dict()
        if collect_bool('Do you want to add params?'):
            while True:
                kvs = collect_key_value_pair('Parameter')
                params[kvs[0]] = kvs[1]
                if not collect_bool('Add another parameter? '):
                    break
        return {
                'logic': {
                    'module': module_name,
                    'parameters': params
                    }
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
        return {
            'id': task_id,
            'logic': get_logic(),
            'inputs': get_ios('Input: '),
            'outputs': get_ios('Output: ')
            }
    trigger = non_empty_prompt('Trigger: ')
    tasks = list()
    while True:
        tasks.append(get_task())
        if not collect_bool('Add another one task? '):
            break
    return {
            'schedule': trigger,
            'tasks': tasks
            }

@click.command(name="create")
@click.option("-n", "--name", required=True, type=str)
@aws
def create_data_product(name):
    try:
        dp_id = non_empty_prompt("Unique data product ID: ", default=name)
        schema = get_schema()
        description = non_empty_prompt('Description: ')
        owner = non_empty_prompt('Owner e-mail: ')
        pipeline = get_pipeline()
        #  schedule = prompt('Schedule: ')
        #  task_id = prompt('Task id: ')
        #  ds_output = prompt('Outputs: ')
        #  def prompt_autocomplete():
        #      app = get_app()
        #      b = app.current_buffer
        #      print(b.complete_state)
        #      if b.complete_state:
        #          b.complete_next()
        #      else:
        #          b.start_completion(select_first=False)

           #  b.insert_text(" ")
        print_formatted_text(
            HTML(f"Product name: <green>{name}</green> and ID:  <green>{dp_id}</green>"),
            style=style
        )
        #  print_formatted_text(HTML(f"Input: <green>{ds_input}</green>"), style=style)
    except Exception as ex:
        traceback.print_exc()
        print(ex)

