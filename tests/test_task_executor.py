import driver
from types import SimpleNamespace
from pyspark.sql import DataFrame
from driver import DataSet
from driver.processors import schema_checker, constraint_processor, transformer_processor


def test_end_to_end(spark_session, transaction_df: DataFrame, fixture_asset_path, app_args):
    dfs = {"some_schema.some_table": transaction_df}

    def mock_input_handler(props: SimpleNamespace):
        return dfs.get(props.table)

    def mock_output_handler(ds: DataSet):
        assert ds.id == 'transaction'
        assert ds.df.count() == transaction_df.count()
        ds.df.show()
        ds.df.describe()

    driver.init(spark_session)
    driver.register_data_source_handler('connection', mock_input_handler)
    driver.register_postprocessors(schema_checker, constraint_processor, transformer_processor)
    driver.register_output_handler('default', mock_output_handler)
    driver.register_output_handler('lake', mock_output_handler)
    setattr(app_args, 'product_path', fixture_asset_path)
    driver.process_product(app_args, fixture_asset_path)

def test_resolve_io_type():
    pass
