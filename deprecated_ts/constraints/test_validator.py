from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
)
from job_interpreter.constraints import ExistsConstraint, ConstraintValidator


def person_data_frame(spark_session):
    return spark_session.createDataFrame(
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


def test_validator_success(spark_session):
    data_frame = person_data_frame(spark_session)
    exist_constraint = ExistsConstraint(data_frame, 'id')

    result = ConstraintValidator([exist_constraint]).validate()

    assert len(result) == 0


def test_validator_failure(spark_session):
    data_frame = person_data_frame(spark_session)
    exist_constraint = ExistsConstraint(data_frame, 'foo')

    result = ConstraintValidator([exist_constraint]).validate()

    assert len(result) != 0
    exists_validation_result = result[0]
    assert exists_validation_result.valid is False
    assert exists_validation_result.expected == 'foo'
    assert exists_validation_result.column == 'foo'
