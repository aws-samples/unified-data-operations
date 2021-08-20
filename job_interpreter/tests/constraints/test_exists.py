from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
)
from ...constraints import ExistsConstraint


def test_exists_true(spark_session):
    data_frame = spark_session.createDataFrame(
        [
            (1, "Joe", "Average"),
            (2, "Max", "Mustermann"),
        ],
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
            ]
        ),
    )

    result = ExistsConstraint(data_frame, 'id').validate()

    assert result.column == 'id'
    assert result.valid is True
    assert result.expected == 'id'
    assert result.actual == 'id'


def test_exists_false(spark_session):
    data_frame = spark_session.createDataFrame(
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

    result = ExistsConstraint(data_frame, 'id').validate()

    assert result.column == 'id'
    assert result.valid is False
    assert result.expected == 'id'
    assert result.actual is None


