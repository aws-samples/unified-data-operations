from driver.core import ConfigContainer
from pyspark.sql import DataFrame
from pytest import fixture
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, LongType, DoubleType, TimestampType

# todo: adapt this to your Environment
DEFAULT_BUCKET = "s3://test-bucket"


@fixture
def app_args() -> ConfigContainer:
    args = ConfigContainer()
    setattr(args, "default_data_lake_bucket", DEFAULT_BUCKET)
    return args


@fixture(scope="module")
def person_relevant_schema() -> StructType:
    return StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("last_name", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
        ]
    )


@fixture(scope="module")
def person_relevant_df(spark_session, person_relevant_schema) -> DataFrame:
    return spark_session.createDataFrame(
        [
            (2666, "Nguyen", "Blake", 6210, "non binary"),
            (5343, "Miller", "Richard", 8524, "male"),
            (6315, "Johnson", "Caitlin", 67, "non binary"),
            (9102, "Rodriguez", "Michael", 460, "non binary"),
            (4281, "Morton", "James", 8900, "female"),
        ],
        person_relevant_schema,
    )


@fixture(scope="module")
def customer_personal_schema() -> StructType:
    return StructType(
        [
        ]
    )
