import click
import boto3
import functools
import re
import driver
import driver.aws.providers
from driver import io_handlers
from prompt_toolkit.shortcuts import CompleteStyle
from .bindings import kb
from driver.aws.providers import connection_provider, datalake_provider
from driver.io_handlers import connection_input_handler, lake_input_handler, file_input_handler
from driver.util import build_spark_configuration, parse_values_into_strict_type
from typing import Optional, Callable
from prompt_toolkit.styles import Style
from prompt_toolkit import HTML, print_formatted_text
from prompt_toolkit.validation import Validator
from prompt_toolkit import prompt
from cli.core import ChainValidator
from argparse import Namespace
from prompt_toolkit.completion import (
    CompleteEvent,
    Completer,
    Completion,
    WordCompleter,
)
from prompt_toolkit.document import Document
from typing import Iterable

#  import driver.aws.providers

io_types = ["connection", "model", "file"]
boto_session = None
style = Style.from_dict(
    {
        "red": "#ff0066",
        "green": "#44ff00",
        "levender": "#E2D1F9",
        "path": "#884444 underline",
        "green_italic": "#44ff00 italic",
    }
)


def aws(func):
    @click.option("-p", "--profile", "aws_profile", type=str, help="AWS profile")
    @click.option("-r", "--region", "aws_region", type=str, help="AWS Region")
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        profile = kwargs.pop("aws_profile")
        region = kwargs.pop("aws_region")
        cid = boto3.client("sts").get_caller_identity()
        account = cid.get("Account")
        global boto_session
        if profile and region:
            boto_session = boto3.Session(profile_name=profile, region_name=region)
        elif profile:
            boto_session = boto3.Session(profile_name=profile)
        elif region:
            boto_session = boto3.Session(region_name=region)
        else:
            boto_session = boto3.Session()
        print_formatted_text(
            HTML(
                f"Using AWS account <green>{account}</green> profile <green>{boto_session.profile_name}</green> "
                f"and region <green>{boto_session.region_name}</green>"
            ),
            style=style,
        )
        driver.aws.providers.init(profile=profile, region=region)
        return func(*args, **kwargs)

    return wrapper


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


def select_from_list(
    prompt_string: str,
    string_list: list[str],
    ignore_case: bool = True,
):
    word_completer = WordCompleter(string_list, ignore_case)
    return prompt(prompt_string, completer=word_completer)


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
            complete_style=CompleteStyle.COLUMN,
            complete_while_typing=True,
        )
    )


def driver_subsystem(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        driver.init(spark_config=build_spark_configuration(Namespace(local=True)))
        io_handlers.init(connection_provider, datalake_provider)
        driver.register_data_source_handler("connection", connection_input_handler)
        driver.register_data_source_handler("model", lake_input_handler)
        driver.register_data_source_handler("file", file_input_handler)
        return func(*args, **kwargs)

    return wrapper


def validated_prompt(request_text: str, default_value: Optional[str], *callables: Callable[[str], bool]):
    chain_validator = ChainValidator(move_cursor_to_end=True, *callables)
    if default_value is not None:
        return prompt(request_text, validator=chain_validator, default=default_value)
    else:
        return prompt(request_text, validator=chain_validator)


def non_empty_prompt(topic_text: str, default: Optional[str] = None):
    non_empty_validator = Validator.from_callable(
        lambda x: x is not None and len(x) > 0, error_message="Please provide a value"
    )
    if default is not None:
        return prompt(topic_text, validator=non_empty_validator, default=default)
    else:
        return prompt(topic_text, validator=non_empty_validator)


def cron_expression_prompt(prompt_text: str) -> str:
    regex = re.compile(
        "(@(annually|yearly|monthly|weekly|daily|hourly|reboot))|(@every (\d+(ns|us|µs|ms|s|m|h))+)|((((\d+,)+\d+|(\d+(\/|-)\d+)|\d+|\*|\?) ?){5,7})"
    )
    cron_expression_validator = Validator.from_callable(
        lambda x: bool(regex.match(x)), error_message="Please provide a valid cron expression", move_cursor_to_end=True
    )
    cron_expressions = [
        "@annually",
        "@yearly",
        "@monthly",
        "@weekly",
        "@daily",
        "@hourly",
        "0 * 0 ? * * *",
        "0 * * ? * * *",
        "0 0 0 ? * * *",
    ]
    meta = {
        "0 * 0 ? * * *": "Every Minute at 0s",
        "0 * * ? * * *": "Every Hour at 0m",
        "0 0 0 ? * * *": "Every Day at 1h",
    }
    return prompt(
        prompt_text,
        validator=cron_expression_validator,
        completer=WordCompleter(words=cron_expressions, meta_dict=meta),
        validate_while_typing=False,
    )


def collect_key_value_pairs(question: str, key_name: str):
    def collect_key_value_pair(topic_text: str):
        keyval_validator = Validator.from_callable(
            lambda x: "=" in x and len(x.split("=")[1]) > 0 and " " not in x,
            error_message="Use an equal sign separator and avoid spaces.",
            move_cursor_to_end=True,
        )
        kvs = prompt(f"{topic_text}: ", validator=keyval_validator, validate_while_typing=False)
        return [val.strip("' ") for val in kvs.split("=")]

    params = dict()
    if collect_bool(question):
        while True:
            kvs = collect_key_value_pair(key_name)
            params[kvs[0]] = parse_values_into_strict_type(kvs[1])
            if not collect_bool(f"Add another {key_name}? "):
                break
    return params


def collect_bool(topic_text: str, default: bool | None = None):
    yes_no_validator = Validator.from_callable(lambda x: x.lower() in ["y", "n"], error_message="Chose Y or N")
    if default is not None:
        decision = prompt(
            f"{topic_text} Y/N: ",
            validator=yes_no_validator,
            default="Y" if default else "N",
            validate_while_typing=False,
        )
    else:
        decision = prompt(f"{topic_text} Y/N: ", validate_while_typing=False)
    return decision.lower() in ["y", "yes"]
