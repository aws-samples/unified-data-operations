import yaml

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
)
from ...transforms import Transformer
from pathlib import Path


def as_yaml(path: str):
    return yaml.safe_load(open(Path(path)))


def test_transform(spark_session):
    data_frame = spark_session.createDataFrame(
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
    model = as_yaml('./job_interpreter/tests/transforms/model_transforms.yml')

    result = Transformer(data_frame).transform(model['models'][0])

    assert result.columns == ['id', 'first_name', 'age_bins']
    data = result.collect()
    assert data[0][0] == 1
    assert data[0][1] == '6dd8b7d7d3c5c4689b33e51b9f10bc6a9be89fe8fa2a127c8c6c03cd05d68ace'
    assert data[0][2] == '(20,40]'
    assert data[1][0] == 2
    assert data[1][1] == 'a1a5936d3b0f8a69fd62c91ed9990d3bd414c5e78c603e2837c65c9f46a93eb8'
    assert data[1][2] == '(40,60]'
