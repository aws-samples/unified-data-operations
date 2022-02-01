import logging
from typing import List
import time
import datetime
from driver.task_executor import DataSet
from pyspark.sql.functions import lit, unix_timestamp

logger = logging.getLogger(__name__)


def execute(inp_dfs: List[DataSet], create_timestamp=False):
    logger.info(f'create timestamp: {create_timestamp}')
    if create_timestamp:
        timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        for ds in inp_dfs:
            ds.df = ds.df.withColumn('time', unix_timestamp(lit(timestamp), 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
    return inp_dfs
