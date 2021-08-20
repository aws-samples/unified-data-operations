from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
)
from ...transforms import SkipTransform


def test_skip(spark_session):
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

    result = SkipTransform(data_frame, 'first_name').transform()

    assert result.first()[0] == 'Average'
    assert result.head(2)[1][0] == 'Mustermann'
