from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
)
from ...transforms import ConcatTransform


def test_concat(spark_session):
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

    result = ConcatTransform(data_frame, ['first_name', 'last_name'], ' ', 'person').transform()

    assert result.first()[0] == 'Joe Average'
    assert result.head(2)[1][0] == 'Max Mustermann'
