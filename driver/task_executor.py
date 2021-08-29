from pathlib import Path
from types import SimpleNamespace
from typing import Tuple, Set
from dataclasses import dataclass
import yaml
from pyspark import SparkContext, SparkFiles
from pyspark.sql import DataFrame

data_src_handlers: dict = dict()
input_preprocessors: dict = dict()
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


def register_input_preprocessor(handler_id: str, handler: callable):
    input_preprocessors.update({handler_id: handler})


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
        model = next(iter([m for m in model_def.models if m.id == inp.props.model]), None)
        input_datasets.append(DataSet(inp.id, inp.props.model, model, load_input(inp)))
    return input_datasets


def preprocess_inputs(inp_dfs: list[DataSet]) -> list[DataSet]:
    return inp_dfs


def transform(inp_dfs: list[DataSet], function_name) -> list[DataSet]:
    return inp_dfs


def sink(o_dfs: list[DataSet]):
    for o in o_dfs:
        handle_output = output_handlers.get(o.model.storage.type)
        handle_output(o.model_id, o.df, o.model.storage.options if o.model and o.model.storage else None)


def execute(task: list, models_def: list) -> list[DataSet]:
    print(f'{task.id}')
    print(f'{task.logic}')

    input_dfs: list[DataSet] = preprocess_inputs(load_inputs(task.input, models_def))
    output_dfs: list[DataSet] = transform(input_dfs, task.logic)
    sink(output_dfs)
