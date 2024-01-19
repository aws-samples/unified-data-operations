# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import functools
import json
import logging
import os
import yaml
import re
import configparser
from driver import driver
from os.path import dirname
from pyspark import SparkConf
from typing import List, Any
from jsonschema import validate, ValidationError, Draft3Validator
from yaml.scanner import ScannerError
from driver.core import ArtefactType, ConfigContainer
from .core import IOType, ResolverException
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    TimestampType,
    DateType,
    DataType,
    NumericType,
    BooleanType,
    DecimalType,
    FloatType,
    ShortType,
    ByteType,
    ArrayType,
    MapType,
)

logger = logging.getLogger(__name__)


def build_spark_configuration(args, config: configparser.RawConfigParser = None, custom_hook: callable = None):
    conf = SparkConf()
    if hasattr(args, "aws_profile"):
        logger.info(f"Setting aws profile: {args.aws_profile}")
        os.environ["AWS_PROFILE"] = args.aws_profile
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    if hasattr(args, "local") and args.local:
        """adding local dependencies"""
        deps_path = os.path.join(dirname(dirname(os.path.abspath(__file__))), "spark_deps")
        local_jars = [file for file in os.listdir(deps_path) if file.endswith(".jar")]
        if hasattr(args, "jars"):
            local_jars.extend([f"{deps_path}/{j}" for j in args.jars.strip().split(",")])
        jars = ",".join([os.path.join(deps_path, j) for j in local_jars])
        conf.set("spark.jars", jars)
    if config:
        spark_jars = "spark jars"
        if spark_jars in config.sections():
            for k, v in config.items(spark_jars):
                conf.set(k, v)
    return custom_hook.enrich_spark_conf(conf) if custom_hook and hasattr(custom_hook, "enrich_spark_conf") else conf


def run_chain(input_payload, *callables: callable):
    functions = list()
    functions.extend(callables)
    result = input_payload
    for func in functions:
        func_name = func.func.__name__ if isinstance(func, functools.partial) else func.__name__
        logger.info(f"chain > executing: {func_name}")
        try:
            result = func(result)
        except Exception as exc:
            logger.error(f"{type(exc).__name__} while executing <{func_name}> with error: {str(exc)}")
            raise
    return result


def convert_model_type_to_spark_type(type_str: str) -> DataType:
    type_map = {
        "string": StringType,
        "integer": IntegerType,
        "long": LongType,
        "double": DoubleType,
        "timestamp": TimestampType,
        "date": DateType,
        "numeric": NumericType,
        "boolean": BooleanType,
        "decimal": DecimalType,
        "float": FloatType,
        "short": ShortType,
        "byte": ByteType,
        "array": ArrayType,
        "map": MapType,
    }
    return type_map.get(type_str.lower(), StringType)


def create_model_columns_from_spark_schema(df: DataFrame) -> list[dict[str, Any]]:
    columns = list()
    expr = re.compile("[_|-]")
    for field in df.schema:
        definition = field.jsonValue()
        col = {
            "id": definition.get("name"),
            # todo: add type mapper
            "type": definition.get("type"),
            "name": expr.sub(" ", definition.get("name", {}).title()),
        }
        if not definition.get("nullable"):
            col.update(constraints=[{"type": "not_null"}])
        columns.append(col)
    return columns


def create_metadata_from_data_frame(df: DataFrame):
    meta = dict()
    for field in df.schema:
        metada_data = field.jsonValue().get("metadata")
        if metada_data is not None:
            for k, v in metada_data.items():
                meta[k] = v
    return meta


def parse_values_into_strict_type(value: str) -> Any:
    match (value.strip("'").lower()):
        case "true" | "yes" | "1" | "y":
            return True
        case "false" | "no" | "0" | "n":
            return False
        case _:
            return value


def create_model_from_spark_schema(
    df: DataFrame, model_id: str, model_version: str, model_name: str, model_description: str
) -> dict[str, Any]:
    return {
        "id": model_id,
        "version": model_version,
        "name": model_name,
        "description": model_description,
        "columns": create_model_columns_from_spark_schema(df),
        "meta": create_metadata_from_data_frame(df),
    }


def load_yaml(file_path: str):
    logger.info(f"loading file {file_path}")
    try:
        with open(rf"{file_path}") as file:
            return yaml.safe_load(file)
    except ScannerError as scerr:
        logger.error(f"Could not read [{file_path}] due to: {str(scerr)}")
        raise scerr


def read_csv(path: str) -> DataFrame:
    return driver.get_spark().read.format("csv").option("mode", "DROPMALFORMED").option("header", "true").load(path)


def write_csv(df: DataFrame, output_path: str, buckets=3) -> None:
    df.coalesce(buckets).write.format("csv").mode("overwrite").options(header="true").save(path=output_path)


def safe_get_property(current_object: Any, nested_property: str):
    """
    Returns the value of a property if the property exists. Othewise it returns None
    :return: Value of the property if the property exists, None otherwise.
    """
    elements = nested_property.split(".")
    last_index = len(elements) - 1
    for count, element in enumerate(elements):
        if count == last_index:
            return getattr(current_object, element) if hasattr(current_object, element) else None
        elif hasattr(current_object, element):
            current_object = getattr(current_object, element)
        else:
            return None
    return None


def test_property(object, nested_property: str):
    """
    Verify if an object has a nested property or not.
    :param object: the object to analyze
    :param nested_property: the nested properties separated by dots (.) (eg. model.storage.location)
    :return: True if the nested property can be found on the object;
    """
    current_object = object
    for element in nested_property.split("."):
        if hasattr(current_object, element):
            current_object = getattr(current_object, element)
        else:
            return False
    return True


def filter_list_by_id(object_list: list[Any], object_id: str):
    data_set = next(iter([m for m in object_list if m.id == object_id]), None)
    if data_set is None:
        raise ValueError(f"There's no <{object_id}> in the list {[m.id for m in object_list]}")
    return data_set

def validate_json_schema(validable_dict: dict, artefact_type: ArtefactType):
    schema_vesion = validable_dict.get("schema_version")
    if not schema_vesion:
        raise ValidationError("schema_version keyword must be provided")
    script_folder = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.join(script_folder, "schema", schema_vesion, f"{artefact_type.name}.json")
    with open(schema_path) as schema:
        schema = json.load(schema)
    try:
        validate(validable_dict, schema)
    except ValidationError as verr:
        for err in sorted(Draft3Validator(schema).iter_errors(validable_dict), key=str):
            logger.error(f"validation error detail: {err.message}")
        logger.error(f"{type(verr).__name__} while checking [{artefact_type.name}]: {str(verr)}")
        raise verr
    return validable_dict


def resolve_io_type(io_definition: ConfigContainer) -> IOType:
    if hasattr(io_definition, IOType.connection.name):
        return IOType.connection
    elif hasattr(io_definition, IOType.file.name):
        return IOType.file
    elif hasattr(io_definition, IOType.model.name):
        return IOType.model
    else:
        raise ResolverException(f"This IO type  is not supported yet: {io_definition.__repr__()}.")


def label_io_types_on_product(product: ConfigContainer) -> ConfigContainer:
    def iterate_over_io(ios: list[ConfigContainer]):
        for io in ios:
            setattr(io, "type", resolve_io_type(io))

    product = product.product if hasattr(product, "product") else product
    for task in product.pipeline.tasks:
        iterate_over_io(task.inputs)
        iterate_over_io(task.outputs)
    return product


def enrich_product(product_input: ConfigContainer, args):
    # todo: replace this with a proper object merging logic
    product = product_input.product
    if not hasattr(product, "defaults"):
        setattr(product, "defaults", ConfigContainer())
    if hasattr(args, "default_data_lake_bucket") and not hasattr(product.defaults, "storage"):
        storage = ConfigContainer()
        setattr(storage, "location", args.default_data_lake_bucket)
        logger.debug(f"product defaults {product.defaults}")
        setattr(product.defaults, "storage", storage)
    if not test_property(product, "defaults.storage.location"):
        setattr(product.defaults.storage, "location", args.default_data_lake_bucket)
    return product


def enrich_models(models: ConfigContainer, product: ConfigContainer):
    def add_back_types(model, extended_model):
        columns_with_missing_type = [col for col in model.columns if not hasattr(col, "type")]
        for col in columns_with_missing_type:
            setattr(col, "type", filter_list_by_id(extended_model.columns, col.id).type)

    def decorate_model_with_defaults(model):
        if hasattr(product, "defaults"):
            if not hasattr(model, "storage") and hasattr(product.defaults, "storage"):
                setattr(model, "storage", product.defaults.storage)
            if not hasattr(model.storage, "location") and hasattr(product.defaults.storage, "location"):
                setattr(model.storage, "location", product.defaults.storage.location)
            if not hasattr(model.storage, "options") and hasattr(product.defaults.storage, "options"):
                setattr(model.storage, "options", product.defaults.storage.options)
        if not hasattr(model.storage, "type"):
            setattr(model.storage, "type", "lake")
        if not hasattr(model.storage, "format"):
            setattr(model.storage, "format", "parquet")
        return model

    compiled_models = list()
    for model in models.models:
        if hasattr(model, "extends"):
            extended_model = filter_list_by_id(models.models, model.extends)
            if not extended_model:
                raise Exception(
                    f"Cannot extend model {model.id} with {extended_model} because the root model is not found."
                )
            current_model_columns = set([col.id for col in model.columns])
            extended_model_columns = set([col.id for col in extended_model.columns])
            inherited_column_ids = extended_model_columns - current_model_columns
            inherited_columns = [filter_list_by_id(extended_model.columns, col_id) for col_id in inherited_column_ids]
            model.columns.extend(inherited_columns)
            add_back_types(model, extended_model)
        compiled_models.append(decorate_model_with_defaults(model))
    return compiled_models


def compile_product(product_path: str, args, prod_def_filename: str = "product.yml"):
    product_path = os.path.join(product_path, prod_def_filename)
    product_processing_chain = [
        load_yaml,
        functools.partial(validate_json_schema, artefact_type=ArtefactType.product),
        ConfigContainer.create_from_dict,
        functools.partial(enrich_product, args=args),
        label_io_types_on_product,
    ]
    return run_chain(product_path, *product_processing_chain)


def compile_models(
    product_path: str, product: ConfigContainer, def_file_name: str = "model.yml"
) -> List[ConfigContainer]:
    model_path = os.path.join(product_path, def_file_name)
    part_validate_schema = functools.partial(validate_json_schema, artefact_type=ArtefactType.model)
    part_enrich_model = functools.partial(enrich_models, product=product)
    model_processing_chain = [load_yaml, part_validate_schema, ConfigContainer.create_from_dict, part_enrich_model]
    return run_chain(model_path, *model_processing_chain)
