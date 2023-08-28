# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


import os
from yaml import load, Loader
from jsonschema import ValidationError, validate as validate_jsonschema
from .error import ConfigurationError
from .core import Column, ColumnType, ConnectionInput, DataProduct, FileInput, Model, ModelAccess, ModelInput, ModelMetadata, ModelOutput, Pipeline, Task
import json

DPP_PATH_PRODUCT_YML = 'product.yml'
DPP_PATH_MODEL_YML = 'model.yml'


def extract_yml(source_path: str):
    path_product_yml = source_path + '/' + DPP_PATH_PRODUCT_YML
    path_model_yml = source_path + '/' + DPP_PATH_MODEL_YML

    with open(path_product_yml, 'r') as product:
        product_data = load(product, Loader=Loader)

    with open(path_model_yml, 'r') as model:
        model_data = load(model, Loader=Loader)

    return product_data, model_data


def get_schema(schema_version: str, schema_type: str):
    if not schema_version:
        raise Exception('schema_version keyword must be provided')

    basepath = os.path.dirname(__file__)
    schema_path = os.path.join(
        basepath, 'schema', schema_version, f'{schema_type}.json')
    with open(schema_path) as schema:
        schema = json.load(schema)
    return schema


def validate(product_yaml: str, model_yaml: str):
    try:
        validate_jsonschema(product_yaml, get_schema(
            product_yaml.get('schema_version', None), 'product'))
        validate_jsonschema(model_yaml, get_schema(
            model_yaml.get('schema_version', None), 'model'))
    except FileNotFoundError as e:
        raise ConfigurationError(
            f'YAML file not found. {str(e).splitlines()[0]}')
    except ValidationError as e:
        raise ConfigurationError(
            f'Invalid YAML file found. {str(e).splitlines()[0]}')
    except Exception as e:
        raise ConfigurationError(e)


def parse(source_path: str) -> DataProduct:
    # load sources from disk
    product_yaml, model_yaml = extract_yml(source_path)

    # compare found sources against jsonschema
    validate(product_yaml, model_yaml)

    # transfer into common model
    product_raw = product_yaml['product']

    ##############
    # product.yml
    ##############
    pipeline = Pipeline(product_raw['pipeline']['schedule'])
    for task_raw in product_raw['pipeline']['tasks']:
        task = Task(task_raw['id'])
        pipeline.add_task(task)

        for input_raw in task_raw['inputs']:
            # file
            if "file" in input_raw:
                task.add_input(
                    FileInput(input_raw['file'], input_raw['model']))

            # connection
            if "connection" in input_raw:
                table_tokens = input_raw["table"].split('.', -1)
                task.add_input(ConnectionInput(
                    input_raw["connection"], table_tokens[0], table_tokens[1]))

            # model
            if ("model" in input_raw) and len(input_raw) == 1:
                model_id_tokens = input_raw["model"].split('.', -1)
                task.add_input(ModelInput(
                    model_id_tokens[0], model_id_tokens[1]))

        for output_raw in task_raw['outputs']:
            task.add_output(ModelOutput(output_raw["model"]))

    dp = DataProduct(product_raw['id'],
                     product_raw.get('name', product_raw['id']),
                     product_raw['version'],
                     product_raw.get('description', None),
                     product_raw['owner'],
                     pipeline)

    ##############
    # model.yml
    ##############
    for model_raw in model_yaml['models']:
        tags_raw = [{'name': k, 'value': v}
                    for k, v in model_raw.get('tags', dict()).items()]

        meta_raw = model_raw.get('meta', None)
        contains_pii = meta_raw.get(
            'contains_pii', False) if meta_raw else False
        metadata = ModelMetadata(contains_pii)

        access_raw = model_raw.get('access', None)
        domain = access_raw.get('domain', None) if access_raw else None
        confidentiality = access_raw.get(
            'confidentiality', None) if access_raw else None
        access = ModelAccess(domain, confidentiality)

        model = Model(model_raw['id'], model_raw.get('name', model_raw['id']),
                      model_raw['version'], model_raw.get('description', None), metadata, access, tags_raw)

        for column_raw in model_raw['columns']:
            constraints = [c.get('type', None)
                           for c in column_raw.get('constraints', list())]
            transforms = [t.get('type', None)
                          for t in column_raw.get('transform', list())]

            # do not add skipped columns
            if 'skip' not in transforms:
                model.add_column(Column(ColumnType[column_raw.get('type', 'string')].value, column_raw['id'], column_raw.get('name', column_raw['id']),
                                        column_raw.get('description', None), constraints, transforms))

        dp.add_model(model)

    return dp
