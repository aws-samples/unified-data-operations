from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)
from ...constraints import RegExpConstraint


def test_regexp_true(spark_session):
    data_frame = spark_session.createDataFrame(
        [
            ("1", "Joe", "Average"),
            ("2", "Max", "Mustermann"),
        ],
        StructType(
            [
                StructField("id", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
            ]
        ),
    )

    result = RegExpConstraint(data_frame, 'id', '^[0-9]$').validate()

    assert result.column == 'id'
    assert result.valid is True
    assert result.expected == 2
    assert result.actual == 2


def test_regexp_false(spark_session):
    data_frame = spark_session.createDataFrame(
        [
            ("1", "Joe", "Average"),
            ("A", "Max", "Mustermann"),
        ],
        StructType(
            [
                StructField("id", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
            ]
        ),
    )

    result = RegExpConstraint(data_frame, 'id', '^[0-9]$').validate()

    assert result.column == 'id'
    assert result.valid is False
    assert result.expected == 2
    assert result.actual == 1


