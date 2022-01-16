import functools
import json
import os
import yaml

from types import SimpleNamespace
from typing import List
from jsonschema import validate, ValidationError, Draft3Validator

from .core import ArtefactType


def run_chain(input_payload, *callables: callable):
    functions = list()
    functions.extend(callables)
    result = input_payload
    for func in functions:
        print(f'Chain > executing: {func.func.__name__ if isinstance(func, functools.partial) else func.__name__}')
        result = func(result)
    return result


def parse_dict_into_object(d: dict):
    x = SimpleNamespace()
    for k, v in d.items():
        if isinstance(v, dict):
            setattr(x, k, parse_dict_into_object(v))
        elif isinstance(v, list):
            object_list = list()
            for e in v:
                object_list.append(parse_dict_into_object(e) if isinstance(e, dict) else e)
            setattr(x, k, object_list)
        else:
            setattr(x, str(k), v)
    return x


def load_yaml(file_path: str):
    print(f'loading file {file_path}')
    with open(fr'{file_path}') as file:
        return yaml.load(file, Loader=yaml.FullLoader)


def filter_list_by_id(object_list, object_id):
    return next(iter([m for m in object_list if m.id == object_id]), None)


def validate_schema(validable_dict: dict, artefact_type: ArtefactType):
    schema_vesion = validable_dict.get('schema_version')
    if not schema_vesion:
        raise ValidationError('schema_version keyword must be provided')
    script_folder = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.join(script_folder, 'schema', schema_vesion, f'{artefact_type}.json')
    with open(schema_path) as schema:
        schema = json.load(schema)
    try:
        validate(validable_dict, schema)
    except ValidationError as verr:
        for err in sorted(Draft3Validator(schema).iter_errors(validable_dict), key=str):
            print(f'validation error detail: {err.message}')
        print(f"{type(verr).__name__}: {str(verr)}")
        raise verr
    return validable_dict


def enrich_product(product_input: SimpleNamespace, args):
    product = product_input.product
    if not hasattr(product, 'defaults'):
        setattr(product, 'defaults', SimpleNamespace())
    if hasattr(args, 'default_data_lake_bucket') and not hasattr(product.defaults, 'storage'):
        storage = SimpleNamespace()
        setattr(storage, 'location', args.default_data_lake_bucket)
        setattr(product.defaults, 'storage', storage)
    return product


def enrich_models(models: SimpleNamespace, product: SimpleNamespace):
    def add_back_types(model, extended_model):
        columns_with_missing_type = [col for col in model.columns if not hasattr(col, 'type')]
        for col in columns_with_missing_type:
            setattr(col, 'type', filter_list_by_id(extended_model.columns, col.id).type)

    compiled_models = list()
    for model in models.models:
        if hasattr(model, 'extends'):
            extended_model = filter_list_by_id(models.models, model.extends)
            if not extended_model:
                raise Exception(
                    f'Cannot extend model {model.id} with {extended_model} because the root model is not found.')
            current_model_columns = set([col.id for col in model.columns])
            extended_model_columns = set([col.id for col in extended_model.columns])
            inherited_column_ids = extended_model_columns - current_model_columns
            inherited_columns = [filter_list_by_id(extended_model.columns, col_id) for col_id in inherited_column_ids]
            model.columns.extend(inherited_columns)
            add_back_types(model, extended_model)
        if not hasattr(model.storage, 'location') and hasattr(product.defaults.storage, 'location'):
            setattr(model.storage, 'location', product.defaults.storage.location)
        compiled_models.append(model)
    return compiled_models


def compile_product(product_path: str, args):
    part_enrich_product = functools.partial(enrich_product, args=args)
    part_validate_schema = functools.partial(validate_schema, artefact_type=ArtefactType.product)
    product_path = os.path.join(product_path, 'product.yml')
    product_processing_chain = [load_yaml, part_validate_schema, parse_dict_into_object, part_enrich_product]
    return run_chain(product_path, *product_processing_chain)


def compile_models(product_path: str, product: SimpleNamespace):
    model_path = os.path.join(product_path, 'model.yml')
    part_validate_schema = functools.partial(validate_schema, artefact_type=ArtefactType.models)
    part_enrich_models = functools.partial(enrich_models, product=product)
    model_processing_chain = [load_yaml, part_validate_schema, parse_dict_into_object, part_enrich_models]
    return run_chain(model_path, *model_processing_chain)
