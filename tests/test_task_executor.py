import json
from types import SimpleNamespace

import os
from pyspark.sql import DataFrame
import driver
from driver.core import DataSet
from driver.processors import schema_checker, constraint_processor, transformer_processor


def test_end_to_end(spark_session, person_df: DataFrame):
    dfs = {"persons": person_df}

    def mock_input_handler(props: SimpleNamespace):
        return dfs.get(props.table)

    def mock_output_handler(ds: DataSet):
        assert ds.model_id == 'person'
        assert ds.df.count() == person_df.count()
        ds.df.show()
        ds.df.describe()

    driver.init(spark_session)
    driver.register_data_source_handler('connection', mock_input_handler)
    driver.register_postprocessors(schema_checker, constraint_processor, transformer_processor)
    driver.register_output_handler('default', mock_output_handler)
    driver.register_output_handler('lake', mock_output_handler)
    driver.process_product(f'{os.path.dirname(os.path.abspath(__file__))}/assets/')
