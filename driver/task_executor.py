# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import importlib
import logging

import sys

from types import SimpleNamespace
from typing import List, Callable, Dict
from .util import filter_list_by_id, enrich_models
from .core import DataSet, DataProduct, IOType, ProcessorChainExecutionException, ValidationException, \
    resolve_data_set_id, ResolverException, resolve_data_product_id

logger = logging.getLogger(__name__)

data_src_handlers: dict = dict()
pre_processors: list = list()
post_processors: list = list()
transformers: dict = dict()
output_handlers: dict = dict()


def register_data_source_handler(src_type_id: str, handler: callable):
    data_src_handlers.update({src_type_id: handler})


def register_preprocessors(*handlers: callable):
    pre_processors.extend(handlers)


def register_postprocessors(*handlers: callable):
    post_processors.extend(handlers)


def register_transformer(transformer_id: str, handler: callable):
    transformers.update({transformer_id: handler})


def add_transformers(additional_transformers: Dict[str, callable]):
    transformers.update(additional_transformers)


def register_output_handler(output_handler_type: str, handler: callable):
    output_handlers.update({output_handler_type: handler})


def resolve_io_type(io_definition: SimpleNamespace) -> IOType:
    if hasattr(io_definition, IOType.connection.name):
        return IOType.connection
    elif hasattr(io_definition, IOType.file.name):
        return IOType.file
    elif hasattr(io_definition, IOType.model.name):
        return IOType.model
    else:
        raise ResolverException(f'This IO type  is not supported yet: {io_definition.__repr__()}.')


def load_inputs(product: SimpleNamespace, inputs: SimpleNamespace, models: List[SimpleNamespace]) -> List[DataSet]:
    input_datasets: list[DataSet] = list()

    def load_input(input_def):
        handle_input = data_src_handlers.get(input_def.type)
        if not handle_input:
            raise Exception(f"Input source handler [{input_def.type}] not registered.")
        return handle_input(input_def)

    for inp in inputs:
        model_id = inp.model if hasattr(inp, 'model') else None
        setattr(inp, 'type', resolve_io_type(inp))
        
        # dataset_id is build as follows
        # file:         <assigned model id OR filename without filetype>
        # model:        <data product id>.<model id>
        # connection:   <connection id>.<table name>
        data_product_id = resolve_data_product_id(inp)
        dataset_id = f'{data_product_id}.{resolve_data_set_id(inp)}' if data_product_id else resolve_data_set_id(inp)
        
        model_obj = filter_list_by_id(models, model_id)
        
        dp = DataProduct(id=product.id, description=getattr(product, 'description', None),
                         owner=getattr(product, 'owner', None))
        input_datasets.append(DataSet(dataset_id, load_input(inp), model_obj, dp))
    return input_datasets


def run_processors(phase: str, datasets: List[DataSet], processors: List[Callable]) -> List[DataSet]:
    try:
        processed_dfs: list[datasets] = datasets
        for processor in processors:
            logger.info(f'-> running processor: [{processor.__name__}]')
            new_dss: list[datasets] = list()
            for ds in processed_dfs:
                new_dss.append(processor(ds))
            processed_dfs = new_dss
        return processed_dfs
    except ValidationException as vex:
        raise ProcessorChainExecutionException(
            f'{type(vex).__name__} in processor [{processor.__name__}] at processor chain: [{phase}]: {str(vex)}') from vex
    except Exception as e:
        raise ProcessorChainExecutionException(
            f'{type(e).__name__} in [{processor.__name__}] at processor chain: [{phase}]: {str(e)}') from e


def transform(inp_dfs: List[DataSet], product_path: str, custom_module_name, params=None) -> List[DataSet]:
    from driver.driver import get_spark
    sys.path.append(product_path)
    logger.info(f'executing custom module: {custom_module_name}')
    custom_module = importlib.import_module(custom_module_name)
    sys.modules[custom_module_name] = custom_module

    spark = get_spark()
    if params:
        return custom_module.execute(inp_dfs, spark, **params)
    else:
        return custom_module.execute(inp_dfs, spark)


def sink(o_dfs: List[DataSet]):
    for out_dataset in o_dfs:
        handle_output = output_handlers.get(out_dataset.storage_type)
        if not handle_output:
            raise Exception(f'Storage handler identified by {out_dataset.storage_type} is not found.')
        handle_output(out_dataset)


def enrich(datasets: List[DataSet], product: SimpleNamespace, models: List[SimpleNamespace]):
    for dataset in datasets:
        if not dataset.product_id:
            dataset.product_id = product.id
        if not dataset.product_owner:
            dataset.product.owner = getattr(product, 'owner', None)
        if dataset.model is None:
            default_model = enrich_models(SimpleNamespace(models=[SimpleNamespace(id=dataset.id)]), product=product)[0]
            model_obj = next(iter([m for m in models if m.id == dataset.id]), default_model)
            dataset.model = model_obj
    return datasets


def filter_output_models(task_outputs: List[SimpleNamespace], models: List[SimpleNamespace]):
    output_model_names = [to.model for to in task_outputs if hasattr(to, 'model')]
    return [model for model in models if model.id in output_model_names]


def execute(product: SimpleNamespace, task: SimpleNamespace, models: List[SimpleNamespace], product_path: str) \
        -> List[DataSet]:
    logger.info(f'Executing tasks > [{task.id}] for data product [{product.id}].')
    output_models = filter_output_models(task.outputs, models)
    input_dfs: list[DataSet] = run_processors('pre', load_inputs(product, task.inputs, models), pre_processors)
    input_dfs = enrich(input_dfs, product, output_models)
    task_logic_module = task.logic.module if hasattr(task, 'logic') and hasattr(task.logic,
                                                                                'module') else 'builtin.ingest'
    task_logic_params = task.logic.parameters.__dict__ if hasattr(task, 'logic') and hasattr(task.logic,
                                                                                             'parameters') else {}
    output_dfs: list[DataSet] = transform(input_dfs, product_path, task_logic_module, task_logic_params)
    output_dfs = enrich(output_dfs, product, output_models)
    sink(run_processors('post', output_dfs, post_processors))
    return output_dfs
