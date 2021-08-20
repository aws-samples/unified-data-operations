import yaml

from pathlib import Path
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
)
from ...constraints import ConstraintColumnMapper, ConstraintRegistry, ExistsConstraint, NotNullConstraint, DistinctConstraint, RegExpConstraint


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


def as_yaml(path: str):
    return yaml.safe_load(open(Path(path)))


def test_map_not_null(spark_session):
    data_frame = person_data_frame(spark_session)
    column = as_yaml('./job_interpreter/tests/constraints/column_constraint_not_null.yml')
    constraint_mapper = ConstraintColumnMapper(ConstraintRegistry(), data_frame)

    constraints = constraint_mapper.map('first_name', column)

    exists_constraint = constraints[0]
    not_null_constraint = constraints[1]

    assert len(constraints) == 2
    assert isinstance(exists_constraint, ExistsConstraint)
    assert exists_constraint.column == 'first_name'
    assert isinstance(not_null_constraint, NotNullConstraint)
    assert not_null_constraint.column == 'first_name'
    assert not_null_constraint.threshold == 0.1


def test_map_regexp(spark_session):
    data_frame = person_data_frame(spark_session)
    column = as_yaml('./job_interpreter/tests/constraints/column_constraint_regexp.yml')
    constraint_mapper = ConstraintColumnMapper(ConstraintRegistry(), data_frame)

    constraints = constraint_mapper.map('first_name', column)

    exists_constraint = constraints[0]
    not_null_constraint = constraints[1]

    assert len(constraints) == 2
    assert isinstance(exists_constraint, ExistsConstraint)
    assert exists_constraint.column == 'first_name'
    assert isinstance(not_null_constraint, RegExpConstraint)
    assert not_null_constraint.column == 'first_name'
    assert not_null_constraint.regexp == '.*'


def test_map_distinct(spark_session):
    data_frame = person_data_frame(spark_session)
    column = as_yaml('./job_interpreter/tests/constraints/column_constraint_distinct.yml')
    constraint_mapper = ConstraintColumnMapper(ConstraintRegistry(), data_frame)

    constraints = constraint_mapper.map('first_name', column)

    exists_constraint = constraints[0]
    distinct_constraint = constraints[1]

    assert isinstance(exists_constraint, ExistsConstraint)
    assert exists_constraint.column == 'first_name'
    assert isinstance(distinct_constraint, DistinctConstraint)
    assert distinct_constraint.column == 'first_name'
