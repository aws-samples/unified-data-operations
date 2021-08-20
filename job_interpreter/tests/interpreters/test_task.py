import os

from awsglue.context import GlueContext
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from unittest.mock import Mock

from ...interpreters import TaskInterpreter


def ticket_frame(spark_session):
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


def provide_file(spark_session, path: str):
    spark_session.sparkContext.addFile(os.path.abspath(path))


def test_single_product_with_connection_as_input(spark_session):
    provide_file(spark_session, './job_interpreter/tests/interpreters/product.yml')
    provide_file(spark_session, './job_interpreter/tests/interpreters/model.yml')
    interpreter = TaskInterpreter(GlueContext(spark_session.sparkContext))
    interpreter.dataset_mapper = Mock()
    interpreter.dataset_writer = Mock()
    interpreter.dataset_mapper.map.return_value = [{
        'data_frame': ticket_frame(spark_session),
        'model': 'person',
        'connection': 'crm'
    }]

    interpreter.interpret('extract_customers')
