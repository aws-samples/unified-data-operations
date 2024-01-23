import os
import driver
from typing import List
from driver.core import ConfigContainer, resolve_data_set_id
from pyspark.sql import DataFrame
from driver import DataSet
from driver.processors import schema_checker, constraint_processor, transformer_processor
from tests.integration import ingest_task


def test_ingest(
    spark_session,
    test_df: DataFrame
):
    inputs = list()
    person_relevant_ds = DataSet(product_id="dms_sample", model_id="person_relevant", df=test_df)
    inputs.append(person_relevant_ds)
    outputs: List[DataSet] = ingest_task.execute(inputs, spark_session, create_timestamp=True)
    for dataset in outputs:
        print("\n"*5)
        print(f'-----> {dataset.id}')
        assert dataset.id in ["customer_personal"]
        # Here you can provide additional assertions
        # assert input_ds.df.count() == output_ds.df.count()
        dataset.df.show()
        dataset.df.describe()


def test_end_to_end(spark_session, spark_context, test_df: DataFrame, app_args):
    current_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../assets/integration_source/")
    print(f"End to end test executed in working folder: {current_folder}")

    def mock_input_handler(input_definition: ConfigContainer):
        dfs = {"dms_sample.sporting_event": test_df}
        return dfs.get(resolve_data_set_id(input_definition))

    def mock_output_handler(dataset: DataSet):
        assert dataset.id in ["sport_events.sporting_event"]
        # Here you can provide the assertions specifc to the output
        # assert dataset.df.count() == 30
        dataset.df.show()
        dataset.df.describe()

    driver.init(spark_session)
    driver.register_data_source_handler("connection", mock_input_handler)
    driver.register_data_source_handler("model", mock_input_handler)
    driver.register_data_source_handler("file", mock_input_handler)
    driver.register_postprocessors(transformer_processor, schema_checker, constraint_processor)
    driver.register_output_handler("default", mock_output_handler)
    driver.register_output_handler("lake", mock_output_handler)
    driver.process_product(app_args, current_folder)

