import importlib
import sys
from types import SimpleNamespace
from typing import List, Callable

from driver.core import DataSet, DataProduct

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


def load_inputs(product_id: str, inputs: SimpleNamespace, model_def: SimpleNamespace) -> List[DataSet]:
    input_datasets: list[DataSet] = list()

    def load_input(input_def):
        handle_input = data_src_handlers.get(input_def.type)
        if not handle_input:
            raise Exception(f"Input source handler [{input_def.type}] not registered.")
        return handle_input(input_def)

    for inp in inputs:
        model_id = inp.model if hasattr(inp, 'model') else None
        model_obj = next(iter([m for m in model_def.models if m.id == model_id]), None)
        input_datasets.append(DataSet(inp.id, load_input(inp), model_id, model_obj, DataProduct(product_id)))
    return input_datasets


def run_processors(datasets: List[DataSet], processors: List[Callable]) -> List[DataSet]:
    processed_dfs: list[datasets] = datasets
    for processor in processors:
        new_dss: list[datasets] = list()
        for ds in processed_dfs:
            new_dss.append(processor(ds))
        processed_dfs = new_dss
    return processed_dfs


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


def get_model(models, model_id):
    return next(iter([m for m in models if m.id == model_id]), None)


def get_output(outputs, id):
    return next(iter([o for o in outputs if o.id == id]), None)


def validate_outputs(datasets: List[DataSet], outputs: SimpleNamespace):
    configured_outputs = [o.id for o in outputs]
    transformed_outputs = [d.id for d in datasets]

    if configured_outputs != transformed_outputs:
        raise Exception(f'Not all configured outputs [{str(transformed_outputs)}] are generated during transformation [{str(configured_outputs)}].')


def assign_models(datasets: List[DataSet], outputs: List[SimpleNamespace], product_id: str, models_def: SimpleNamespace, output_bucket: str):
    if len(datasets) == 1 and len(outputs) == 1:
        datasets[0].id = outputs[0].id

    for dataset in datasets:
        if dataset.id is None:
            raise Exception(f'Dataset id is undefined. Unable to assign model.')
        output = get_output(outputs, dataset.id)
        model = get_model(models_def.models, output.model)

        if not dataset.product_id:
            dataset.product_id = product_id
        if not dataset.output_bucket:
            dataset.output_bucket = output_bucket

        dataset.model_id = model.id
        dataset.model = model

    return datasets


def execute(product_id: str, task: list, models_def: list, product_path: str, output_bucket: str) -> List[DataSet]:
    print(f'Executing task > {product_id} {task.id}')
    input_dfs: list[DataSet] = run_processors(load_inputs(product_id, task.inputs, models_def), pre_processors)
    task_logic_module = task.logic.module if hasattr(task, 'logic') and hasattr(task.logic, 'module') else 'builtin.ingest'
    task_logic_params = task.logic.parameters.__dict__ if hasattr(task, 'logic') and hasattr(task.logic, 'parameters') else {}
    output_dfs: list[DataSet] = transform(input_dfs, product_path, task_logic_module, task_logic_params)
    output_dfs = assign_models(output_dfs, task.outputs, product_id, models_def, output_bucket)
    validate_outputs(output_dfs, task.outputs)
    sink(run_processors(output_dfs, post_processors))
