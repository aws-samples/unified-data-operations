from operator import contains
import click
import functools

import prompt_toolkit
from pyspark.sql.functions import lower
import driver
import driver.aws.providers
from prompt_toolkit.styles import Style
from prompt_toolkit import HTML, print_formatted_text
from prompt_toolkit.validation import Validator
from prompt_toolkit import prompt

style = Style.from_dict(
    {
        "red": "#ff0066",
        "green": "#44ff00",
        "green_italic": "#44ff00 italic",
    }
)

def aws(func):
    @click.option("-p", "--profile", "aws_profile", type=str)
    @click.option("-r", "--region", "aws_region", type=str)
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        profile = kwargs.pop("aws_profile")
        region = kwargs.pop("aws_region")
        print_formatted_text(HTML(f"Using AWS profile <green>{profile}</green> and region <green>{region}</green>"), style=style)
        driver.aws.providers.init(profile=profile, region=region)
        return func(*args, **kwargs)
    return wrapper

def non_empty_prompt(topic_text: str, default: str|None = None):
    non_empty_validator = Validator.from_callable(lambda x: x is not None and len(x)>0,
                                                  error_message='Please provide a value')
    if default is not None:
        return prompt(topic_text, validator=non_empty_validator, default=default)
    else:
        return prompt(topic_text, validator=non_empty_validator)

def collect_key_value_pair(topic_text: str):
    semicolon_validator = Validator.from_callable(lambda x: ':' in x and len(x.split(':')[1])>0,
                                                  error_message='Use a semicolon separator.')
    kvs = prompt(f'{topic_text}: ', validator=semicolon_validator)
    return [val.strip() for val in kvs.split(':')]

def collect_bool(topic_text: str, default: bool | None = None):
    yes_no_validator = Validator.from_callable(lambda x: x.lower() in ['y', 'n'], error_message = 'Chose Y or N')
    if default is not None:
        decision = prompt(f'{topic_text} Y/N: ', validator=yes_no_validator, default='Y' if default else 'N')
    else:
        decision = prompt(f'{topic_text} Y/N: ')
    return decision.lower() in ['y', 'yes']
