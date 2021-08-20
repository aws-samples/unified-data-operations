from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
)
from ...constraints import DistinctConstraint


def test_distinct_true(spark_session):
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

    result = DistinctConstraint(data_frame, 'id').validate()

    assert result.column == 'id'
    assert result.valid is True
    assert result.expected == 2
    assert result.actual == 2


def test_distinct_false(spark_session):
    data_frame = spark_session.createDataFrame(
        [
            (1, "Joe", "Average"),
            (1, "Max", "Mustermann"),
        ],
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
            ]
        ),
    )

    result = DistinctConstraint(data_frame, 'id').validate()

    assert result.column == 'id'
    assert result.valid is False
    assert result.expected == 2
    assert result.actual == 1


