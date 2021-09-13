from typing import List

from driver.task_executor import DataSet


def execute(inp_dfs: List[DataSet], create_timestamp=False):
    print(f'create timestamp: {create_timestamp}')
    return inp_dfs
