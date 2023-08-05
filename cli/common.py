import click
import boto3
import functools
import driver
import driver.aws.providers
from driver import io_handlers

#  import driver.aws.providers
from driver.aws.providers import connection_provider, datalake_provider
from driver.io_handlers import connection_input_handler, lake_input_handler, file_input_handler
from pyspark import SparkConf
from typing import Optional, Callable
from prompt_toolkit.styles import Style
from prompt_toolkit import HTML, print_formatted_text
from prompt_toolkit.validation import Validator
from prompt_toolkit import prompt
from cli.core import ChainValidator

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
                f"Using AWS account <green>{account}</green> profile <green>{boto_session.profile_name}</green> and region <green>{boto_session.region_name}</green>"
            ),
            style=style,
        )
        driver.aws.providers.init(profile=profile, region=region)
        return func(*args, **kwargs)

    return wrapper


def driver_subsystem(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        driver.init(spark_erbgfucgjrconfig=SparkConf())
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


def collect_key_value_pairs(question: str, key_name: str):
    def collect_key_value_pair(topic_text: str):
        keyval_validator = Validator.from_callable(
            lambda x: "=" in x and len(x.split("=")[1]) > 0, error_message="Use an equal sign separator."
        )
        kvs = prompt(f"{topic_text}: ", validator=keyval_validator, validate_while_typing=False)
        return [val.strip("' ") for val in kvs.split("=")]

    params = dict()
    if collect_bool(question):
        while True:
            kvs = collect_key_value_pair(key_name)
            params[kvs[0]] = kvs[1]
            if not collect_bool(f"Add another {key_name}? "):
                break
    return params


def collect_bool(topic_text: str, default: bool | None = None):
    yes_no_validator = Validator.from_callable(lambda x: x.lower() in ["y", "n"], error_message="Chose Y or N")
    if default is not None:
        decision = prompt(f"{topic_text} Y/N: ", validator=yes_no_validator, default="Y" if default else "N")
    else:
        decision = prompt(f"{topic_text} Y/N: ")
    return decision.lower() in ["y", "yes"]
