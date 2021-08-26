from pathlib import Path
from typing import Tuple, Set

import yaml
from pyspark import SparkContext, SparkFiles
from pyspark.sql import DataFrame

def execute_task(task_def: dict) -> Set[Tuple[str, DataFrame]]:
    def load_input(input_def: dict) -> Set[Tuple[str, DataFrame]]:
        pass

    def preprocess_input(input: Set[Tuple[str, DataFrame]]) -> Set[Tuple[str, DataFrame]]:
        pass

    def transform(input: Set[Tuple[str, DataFrame]], function_name) -> Set[Tuple[str, DataFrame]]:
        pass

    def sink(output: Set[Tuple[str, DataFrame]]):
        pass

    print(f'{task_def.get("input")}')
    print(f'{task_def.get("logic")}')

    input: Set[Tuple[str, DataFrame]] = preprocess_input(load_input(task_def.get('input')))
    output: Set[Tuple[str, DataFrame]] = transform(input, task_def.get("logic"))
    sink(output)