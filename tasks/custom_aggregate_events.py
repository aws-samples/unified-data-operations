import time
import datetime
from typing import List

from pyspark.sql.functions import concat, col, lit, unix_timestamp
from driver.common import get_data_set
from driver.task_executor import DataSet


def execute(inp_dfs: List[DataSet], create_timestamp=False):
    ds = get_data_set(inp_dfs, 'person')
    if ds:
        ds.df = ds.df.withColumn('full_name', concat(col('last_name'), lit(', '), col('first_name')))
    return inp_dfs
