import importlib
import sys

from types import SimpleNamespace
from typing import List, Callable
from .util import filter_list_by_id
from .core import DataSet, DataProduct, IOType, ProcessorChainExecutionException, ValidationException

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


def register_output_handler(output_handler_type: str, handler: callable):
    output_handlers.update({output_handler_type: handler})


def resolve_io_type(io_definition: SimpleNamespace) -> IOType:
    return IOType.connection if hasattr(io_definition, IOType.connection.name) else IOType.model


def resolve_data_set_id(io_def: SimpleNamespace, io_type: IOType) -> str:
    if io_type == IOType.model:
        return getattr(io_def, io_type)
    else:
        table_name_elements = io_def.table.rsplit('.')
        return table_name_elements[len(table_name_elements) - 1]


def load_inputs(product_id: str, inputs: SimpleNamespace, models: List[SimpleNamespace]) -> List[DataSet]:
    input_datasets: list[DataSet] = list()

    def load_input(input_def):
        handle_input = data_src_handlers.get(input_def.type)
        if not handle_input:
            raise Exception(f"Input source handler [{input_def.type}] not registered.")
        return handle_input(input_def)

    for inp in inputs:
        model_id = inp.model if hasattr(inp, 'model') else None
        setattr(inp, 'type', resolve_io_type(inp))
        dataset_id = resolve_data_set_id(inp, inp.type)
        model_obj = filter_list_by_id(models, model_id)
        input_datasets.append(DataSet(dataset_id, load_input(inp), model_obj, DataProduct(product_id)))
    return input_datasets


def run_processors(phase: str, datasets: List[DataSet], processors: List[Callable]) -> List[DataSet]:
    try:
        processed_dfs: list[datasets] = datasets
        for processor in processors:
            print(f'-> running processor: [{processor.__name__}]')
            new_dss: list[datasets] = list()
            for ds in processed_dfs:
                new_dss.append(processor(ds))
            processed_dfs = new_dss
        return processed_dfs
    except ValidationException as vex:
        raise ProcessorChainExecutionException(
            f'{type(vex).__name__} in processor [{processor.__name__}] with dataset [{vex.data_set.id}] at processor chain: [{phase}]') from vex
    except Exception as e:
        raise ProcessorChainExecutionException(
            f'{type(e).__name__} in [{processor.__name__}] at processor chain: [{phase}]') from e


def transform(inp_dfs: List[DataSet], product_path: str, custom_module_name, params=None) -> List[DataSet]:
    sys.path.append(product_path)
    print('executing module: ' + custom_module_name)
    custom_module = importlib.import_module(custom_module_name)
    sys.modules[custom_module_name] = custom_module
    if params:
        return custom_module.execute(inp_dfs, **params)
    else:
        return custom_module.execute(inp_dfs)


def sink(o_dfs: List[DataSet]):
    for out_dataset in o_dfs:
        handle_output = output_handlers.get(out_dataset.storage_type)
        if not handle_output:
            raise Exception(f'Storage handler identified by {out_dataset.storage_type} is not found.')
        handle_output(out_dataset)


def enrich(datasets, product_id, models: List[SimpleNamespace]):
    for dataset in datasets:
        if not dataset.product_id:
            dataset.product_id = product_id
        if dataset.model is None:
            model_obj = next(iter([m for m in models if m.id == dataset.id]), None)
            dataset.model = model_obj
    return datasets


def execute(product_id: str, task: list, models: List[SimpleNamespace], product_path: str) -> List[DataSet]:
    print(f'Executing task > {product_id} {task.id}')
    input_dfs: list[DataSet] = run_processors('pre', load_inputs(product_id, task.inputs, models), pre_processors)
    task_logic_module = task.logic.module if hasattr(task, 'logic') and hasattr(task.logic,
                                                                                'module') else 'builtin.ingest'
    task_logic_params = task.logic.parameters.__dict__ if hasattr(task, 'logic') and hasattr(task.logic,
                                                                                             'parameters') else {}
    output_dfs: list[DataSet] = transform(input_dfs, product_path, task_logic_module, task_logic_params)
    output_dfs = enrich(output_dfs, product_id, models)
    sink(run_processors('post', output_dfs, post_processors))
