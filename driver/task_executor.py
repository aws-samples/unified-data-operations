import importlib
import sys
from types import SimpleNamespace
from typing import List, Callable

from driver.core import DataSet

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
        model_obj = next(iter([m for m in model_def.models if m.id == inp.model]), None)
        input_datasets.append(DataSet(inp.id, inp.model, model_obj, load_input(inp), product_id))
    return input_datasets


def run_processors(datasets: List[DataSet], processors: List[Callable]) -> List[DataSet]:
    processed_dfs: list[datasets] = datasets
    for processor in processors:
        new_dss: list[datasets] = list()
        for ds in processed_dfs:
            new_dss.append(processor(ds))
        processed_dfs = new_dss
    return processed_dfs


def transform(inp_dfs: List[DataSet], custom_module_name, params=None) -> List[DataSet]:
    if custom_module_name not in sys.modules:
        spec = importlib.util.find_spec(custom_module_name)
        if spec is None:
            print(
                f"Warning > skipping custom transformation logic, because module {custom_module_name} "
                f"cannot be found on the classpath")
            return inp_dfs
        else:
            # module = importlib.util.module_from_spec(spec)
            # spec.loader.exec_module(module)
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


def execute(product_id: str, task: list, models_def: list) -> List[DataSet]:
    print(f'Executing task > {product_id} {task.id}')
    input_dfs: list[DataSet] = run_processors(load_inputs(product_id, task.input, models_def), pre_processors)
    task_logic_params = task.logic.params.__dict__ if hasattr(task.logic, 'params') else None
    output_dfs: list[DataSet] = transform(input_dfs, task.logic.func, task_logic_params)
    sink(run_processors(output_dfs, post_processors))
