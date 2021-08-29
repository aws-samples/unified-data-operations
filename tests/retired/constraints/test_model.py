import yaml

from pathlib import Path
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
)
from job_interpreter.constraints import ConstraintModelMapper, ExistsConstraint, DistinctConstraint


def as_yaml(path: str):
    return yaml.safe_load(open(Path(path)))


def test_map(spark_session):
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
    dataset = as_yaml('./deprecated/tests/constraints/dataset_constraints.yml')
    constraint_mapper = ConstraintModelMapper(data_frame)

    constraints = constraint_mapper.map(dataset.get('models')[0])

    exists_constraint = constraints[0]
    not_null_constraint = constraints[1]

    assert len(constraints) == 2
    assert isinstance(exists_constraint, ExistsConstraint)
    assert exists_constraint.column == 'id'
    assert isinstance(not_null_constraint, DistinctConstraint)
    assert not_null_constraint.column == 'id'
