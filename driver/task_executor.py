import importlib
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Tuple, Set
from dataclasses import dataclass
import yaml
from pyspark import SparkContext, SparkFiles
from pyspark.sql import DataFrame

data_src_handlers: dict = dict()
pre_processors: list = list()
post_processors: list = list()
transformers: dict = dict()
output_handlers: dict = dict()


@dataclass
class DataSet:
    id: str
    model_id: str
    model: SimpleNamespace
    df: DataFrame


def register_data_source_handler(src_type_id: str, handler: callable):
    data_src_handlers.update({src_type_id: handler})


def register_preprocessors(*handlers: callable):
    pre_processors.extend(handlers)


def register_postprocessors(*handlers: callable):
    post_processors.extend(handlers)


def register_transformer(transformer_id: str, handler: callable):
    transformers.update({transformer_id: handler})


def register_output_handler(output_handler_type: str, handler: callable):
    output_handlers.update({output_handler_type: handler})


def load_inputs(inputs: SimpleNamespace, model_def: SimpleNamespace) -> list[DataSet]:
    input_datasets: list[DataSet] = list()

    def load_input(input_def):
        handle_input = data_src_handlers.get(input_def.type)
        if not handle_input:
            raise Exception(f"Input source handler [{input_def.type}] not registered.")
        return handle_input(input_def.props)

    for inp in inputs:
        model_obj = next(iter([m for m in model_def.models if m.id == inp.props.model]), None)
        input_datasets.append(DataSet(inp.id, inp.props.model, model_obj, load_input(inp)))
    return input_datasets


def run_processors(datasets: list[DataSet], processors: list[callable]) -> list[DataSet]:
    processed_dfs: list[datasets] = datasets
    for processor in processors:
        new_dss: list[datasets] = list()
        for ds in processed_dfs:
            new_dss.append(processor(ds))
        processed_dfs = new_dss
    return processed_dfs


def transform(inp_dfs: list[DataSet], custom_module_name) -> list[DataSet]:
    if custom_module_name not in sys.modules:
        spec = importlib.util.find_spec(custom_module_name)
        if spec is None:
            print(f"Warning > skipping custom transformation logic, becasue module {custom_module_name} cannot be found on the classpath")
            return inp_dfs
        else:
            # custom_module = importlib.import_module(custom_module_name)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            sys.modules[custom_module_name] = module
            return module.execute(inp_dfs)


def sink(o_dfs: list[DataSet]):
    for o in o_dfs:
        storage_id = o.model.storage.type if (hasattr(o, 'model') and hasattr(o.model, 'storage')) else 'default'
        handle_output = output_handlers.get(storage_id)
        if not handle_output:
            raise Exception(f'Storage handler identified by {storage_id} is not found.')
        handle_output(o.model_id, o.df, o.model.storage.options if o.model and o.model.storage else None)


def execute(task: list, models_def: list) -> list[DataSet]:
    print(f'{task.id}')
    print(f'{task.logic}')

    input_dfs: list[DataSet] = run_processors(load_inputs(task.input, models_def), pre_processors)
    output_dfs: list[DataSet] = transform(input_dfs, task.logic)
    sink(run_processors(output_dfs, post_processors))
