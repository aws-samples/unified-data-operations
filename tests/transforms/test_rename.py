from pyspark.sql.types import (
    StringType,
    StructField,
    StructType
)
from job_interpreter.transforms import RenameTransform


def person_frame(spark_session):
    return spark_session.createDataFrame(
        [
            ("Joe", "Average"),
            ("Max", "Mustermann"),
        ],
        StructType(
            [
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
            ]
        ),
    )


def test_rename(spark_session):
    data_frame = person_frame(spark_session)

    result = RenameTransform(data_frame, 'first_name', 'name').transform()

    assert result.columns == ['name', 'last_name']
