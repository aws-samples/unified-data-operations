
import time
import datetime
from typing import List
from pyspark.sql.functions import concat, col, lit, unix_timestamp
from driver.util import filter_list_by_id
from driver.task_executor import DataSet
from pyspark.sql import SparkSession, Row


def execute(inp_dfs: List[DataSet], spark_session: SparkSession, create_timestamp=True) -> List[DataSet]:
    person_relevant_ds = filter_list_by_id(inp_dfs, 'dms_sample.person_relevant')
    # example of custom logic, adding a new timestamp column to the processed data
    timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    data_frame = person_relevant_ds.df
    data_frame = data_frame.withColumn('time', unix_timestamp(lit(timestamp), 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))

    customer_personal_output = DataSet(model_id='customer_personal', df=data_frame)
    return [customer_personal_output]
