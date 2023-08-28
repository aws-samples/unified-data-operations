# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List
import time
import datetime
from driver.task_executor import DataSet
from pyspark.sql.functions import lit, unix_timestamp
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


def execute(inp_datasets: List[DataSet], spark_session: SparkSession, create_timestamp=False):
    # todo: review and remove this
    #  def resolve_data_set_id(ds: DataSet):
    #      model_id_raw = None
    #      if ds.model:
    #          model_id_raw = ds.model.id
    #      else:
    #          model_id_raw = ds.id

    #      id_tokens = model_id_raw.split(".")

    #      return id_tokens[len(id_tokens) - 1]

    logger.info(f"create timestamp: {create_timestamp}")
    if create_timestamp:
        timestamp = datetime.datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")
        for ds in inp_datasets:
            ds.df = ds.df.withColumn(
                "ingest_date", unix_timestamp(lit(timestamp), "yyyy-MM-dd HH:mm:ss").cast("timestamp")
            )
    return [DataSet(model_id=ds.model_id, df=ds.df) for ds in inp_datasets]
