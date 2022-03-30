import os

from awsglue.context import GlueContext
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from unittest.mock import Mock

from job_interpreter.interpreters import TaskInterpreter


def input_data_frame(spark_session):
    return spark_session.createDataFrame(
        [
            (1, "Joe Average", "Joe", "Average"),
            (2, "Max Mustermann", "Max", "Mustermann"),
        ],
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("full_name", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
            ]
        ),
    )


def output_data_frame(spark_session):
    return spark_session.createDataFrame(
        [
            (1, "c5f6909781a8ea923b51570764be0017ff7e34e7b95aa3cd423677bc799476e8"),
            (2, "dddfab9b5b8a360150547065daff114ff218b39c8b0986b761075977aeeca3c3"),
        ],
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("full_name", StringType(), True)
            ]
        ),
    )


def provide_file(spark_session, path: str):
    spark_session.sparkContext.addFile(os.path.abspath(path))


def test_single_product_with_connection_as_input(spark_session):
    provide_file(spark_session, './deprecated/tests/interpreters/product_wrong_output.yml')
    provide_file(spark_session, './deprecated/tests/interpreters/model_compilation.yml')
    interpreter = TaskInterpreter(GlueContext(spark_session.sparkContext))
    interpreter.dataset_mapper = Mock()
    interpreter.dataset_writer.data_frame_writer = Mock()
    interpreter.dataset_mapper.map.return_value = [{
        'data_frame': input_data_frame(spark_session),
        'model': 'person',
        'connection': 'crm'
    }]

    interpreter.interpret('extract_customers')

    expected_data_frame = output_data_frame(spark_session)
    args, kwargs = interpreter.dataset_writer.data_frame_writer.write.call_args

    actual_data_frame = args[0]
    actual_options = args[1]

    assert actual_data_frame.schema == expected_data_frame.schema
    assert actual_data_frame.collect() == expected_data_frame.collect()
    assert actual_options == {
        'skip_first_row': True,
        'partition_by': 'seat_row',
        'bucketed_at': '512M',
        'stored_as': 'parquet',
        'location': 's3://some_data_lake/some_folder'
    }

