from boto3.session import Session
from pyspark.sql import DataFrame
from pytest import fixture
from deprecated import CatalogService
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
)
from driver.task_executor import DataSet
from types import SimpleNamespace
from unittest import skip


@fixture
def person_frame(spark_session) -> DataFrame:
    return spark_session.createDataFrame(
        [
            (1, "Joe", "Average", 22),
            (2, "Max", "Mustermann", 45),
        ],
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("age", IntegerType(), True)
            ]
        ),
    )


@skip("Integration test is skipped for now")
def test_update(person_frame: DataFrame):
    catalog_service = CatalogService(Session(profile_name='finn'))

    catalog_service.drain_database('customers')

    catalog_service.update_database('customers', 'person', DataSet(
        id='person',
        df=person_frame,
        product_id='customers',
        model_id='person',
        model=SimpleNamespace(
            storage=SimpleNamespace(
                options=SimpleNamespace(
                    location='s3://job-interpreter/data/customers'
                )
            )
        )
    ))
