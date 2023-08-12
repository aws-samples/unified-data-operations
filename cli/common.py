from ast import parse
import click
import boto3
import functools

from pip._vendor.pyparsing.core import Word
from prompt_toolkit.completion.word_completer import WordCompleter
import driver
import driver.aws.providers
from driver import io_handlers
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
import re

#  import driver.aws.providers

boto_session = None
style = Style.from_dict(
    {
        "red": "#ff0066",
        "green": "#44ff00",
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
        "(@(annually|yearly|monthly|weekly|daily|hourly|reboot))|(@every (\d+(ns|us|µs|ms|s|m|h))+)|((((\d+,)+\d+|(\d+(\/|-)\d+)|\d+|\*) ?){5,7})"
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
