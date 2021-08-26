from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
)
from job_interpreter.transforms import HashTransform


def person_frame(spark_session):
    return spark_session.createDataFrame(
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


def test_hash_with_salt(spark_session):
    data_frame = person_frame(spark_session)

    result = HashTransform(data_frame, 'first_name', 'secret').transform()

    assert result.first()[0] == '849a306c0bd98a4fb81828346130b68f9ad5b8536a9569cafdbb17de803235dc'
    assert result.head(2)[1][0] == '5a6b60641ed798456ac92c89d9fd5e19604309d8974f20c83728fe59eea399eb'


def test_hash_without_salt(spark_session):
    data_frame = person_frame(spark_session)

    result = HashTransform(data_frame, 'first_name').transform()

    assert result.first()[0] == '6dd8b7d7d3c5c4689b33e51b9f10bc6a9be89fe8fa2a127c8c6c03cd05d68ace'
    assert result.head(2)[1][0] == 'a1a5936d3b0f8a69fd62c91ed9990d3bd414c5e78c603e2837c65c9f46a93eb8'
