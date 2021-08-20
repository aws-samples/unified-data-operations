from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
)
from ...constraints import NotNullConstraint


def test_not_null_false(spark_session):
    data_frame = spark_session.createDataFrame(
        [
            (1, "Joe", "Average"),
            (None, "Max", "Mustermann"),
        ],
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
            ]
        ),
    )

    result = NotNullConstraint(data_frame, 'id', 0.1).validate()

    assert result.column == 'id'
    assert result.valid is False
    assert result.expected == 0.1
    assert result.actual == 0.5


def test_not_null_true(spark_session):
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

    result = NotNullConstraint(data_frame, 'id', 0.1).validate()

    assert result.column == 'id'
    assert result.valid is True
    assert result.expected == 0.1
    assert result.actual == 0.0


def test_not_null_default_threshold_true(spark_session):
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

    result = NotNullConstraint(data_frame, 'id').validate()

    assert result.column == 'id'
    assert result.valid is True
    assert result.expected == 0.0
    assert result.actual == 0.0


def test_not_null_default_threshold_false(spark_session):
    data_frame = spark_session.createDataFrame(
        [
            (1, "Joe", "Average"),
            (None, "Max", "Mustermann"),
        ],
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
            ]
        ),
    )

    result = NotNullConstraint(data_frame, 'id').validate()

    assert result.column == 'id'
    assert result.valid is False
    assert result.expected == 0.0
    assert result.actual == 0.5
