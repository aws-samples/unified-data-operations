import json
from types import SimpleNamespace

import os
import yaml
from pyspark.sql import DataFrame

from pytest import fixture

import driver
from driver.processors import schema_validator
from .setup_test import *


def test_end_to_end(spark_session, person_df: DataFrame):
    dfs = {"persons": person_df}

    def mock_input_handler(props: SimpleNamespace):
        return dfs.get(props.table)

    def mock_output_handler(model_id: str, df: DataFrame, options: SimpleNamespace):
        assert model_id == 'persons'
        assert df.count() == person_df.count()
        df.show()
        df.describe()

    driver.init(spark_session)
    driver.register_data_source_handler('connection', mock_input_handler)
    driver.register_postprocessors(schema_validator)
    driver.register_output_handler('default', mock_output_handler)
    driver.register_output_handler('lake', mock_output_handler)
    driver.process_product(f'{os.path.dirname(os.path.abspath(__file__))}/assets/')
