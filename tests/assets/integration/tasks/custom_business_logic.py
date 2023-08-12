import time
import datetime
from typing import List

from pyspark.sql.functions import concat, col, lit, unix_timestamp
from driver.task_executor import DataSet


def execute(inp_dfs: List[DataSet], create_timestamp=False):
    ds = DataSet.find_by_id(inp_dfs, "person_relevant")

    if create_timestamp:
        timestamp = datetime.datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")
        ds.df = ds.df.withColumn("time", unix_timestamp(lit(timestamp), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))

    df = ds.df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))

    ds_pub = DataSet(id="person_pub", df=df)
    ds_pii = DataSet(id="person_pii", df=df)

    return [ds_pub, ds_pii]

