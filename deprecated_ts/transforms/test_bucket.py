from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
)
from job_interpreter.transforms import BucketTransform


def person_frame(spark_session):
    return spark_session.createDataFrame(
        [
            ("Billy", "The Kid", 10),
            ("Joe", "Average", 25),
            ("Max", "Mustermann", 34),
            ("John", "Doe", 50),
        ],
        StructType(
            [
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        ),
    )


def test_bucket(spark_session):
    data_frame = person_frame(spark_session)

    result = BucketTransform(data_frame, 'age', [20, 30, 40]).transform()

    data = result.collect()
    assert data[0][2] == '(-inf,20]'
    assert data[1][2] == '(20,30]'
    assert data[2][2] == '(30,40]'
    assert data[3][2] == '(40,inf]'
