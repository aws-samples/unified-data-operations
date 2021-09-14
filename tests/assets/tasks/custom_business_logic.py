import time
import datetime
from typing import List

from pyspark.sql.functions import concat, col, lit, unix_timestamp
from driver.common import get_data_set
from driver.task_executor import DataSet


def execute(inp_dfs: List[DataSet], create_timestamp=False):
    ds = get_data_set(inp_dfs, 'persons')
    if ds:
        ds.df = ds.df.withColumn('full_name', concat(col('first_name'), lit(' '), col('last_name')))
        if create_timestamp:
            timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            ds.df = ds.df.withColumn('time', unix_timestamp(lit(timestamp), 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
    return inp_dfs
