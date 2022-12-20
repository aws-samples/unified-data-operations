# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from typing import List
import time
import datetime
from driver.task_executor import DataSet
from pyspark.sql.functions import lit, unix_timestamp

logger = logging.getLogger(__name__)


def execute(inp_datasets: List[DataSet], create_timestamp=False):
    def resolve_data_set_id(ds: DataSet):
        ds_id_full = ds.id.split('.')
        return ds.model.id if ds.model else ds_id_full[len(ds_id_full)-1]

    logger.info(f'create timestamp: {create_timestamp}')
    if create_timestamp:
        timestamp = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        for ds in inp_datasets:
            ds.df = ds.df.withColumn('ingest_date', unix_timestamp(lit(timestamp), 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))
    return [DataSet(id=resolve_data_set_id(ds), df=ds.df) for ds in inp_datasets]
