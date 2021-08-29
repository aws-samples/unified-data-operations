from types import SimpleNamespace

import pytest, os
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
    LongType,
    DoubleType
)
import driver


@pytest.fixture
def movie_schema():
    return StructType([
        StructField('movieId', IntegerType(), True),
        StructField('title', StringType(), True),
        StructField('genres', StringType(), True)
    ])


@pytest.fixture
def ratings_schema():
    return StructType([
        StructField('userId', IntegerType(), True),
        StructField('movieId', IntegerType(), True),
        StructField('rating', IntegerType(), True),
        StructField('timestamp', LongType(), True)
    ])


@pytest.fixture
def result_schema():
    return StructType([
        StructField('title', StringType(), True),
        StructField('weight_avg', DoubleType(), True),
        StructField('num_votes', IntegerType(), True)
    ])


@pytest.fixture
def movies_df(spark_session, movie_schema):
    return spark_session.createDataFrame([(1, 'Jumanji(1995)', 'Adventure | Children | Fantasy'),
                                          (2, 'Heat (1995)', 'Action|Crime|Thriller')],
                                         movie_schema)


@pytest.fixture
def ratings_df(spark_session, ratings_schema):
    return spark_session.createDataFrame([(1, 1, 4, 1256677221),
                                          (2, 1, 4, 1256677222),
                                          (3, 1, 1, 1256677222),
                                          (4, 2, 4, 1256677222)
                                          ], ratings_schema)


def test_end_to_end(spark_session, movies_df: DataFrame, ratings_df: DataFrame):
    dfs = {"movies": movies_df, "ratings": ratings_df}

    def mock_input_handler(props: SimpleNamespace):
        return dfs.get(props.table)

    def mock_output_handler(model_id:str, df: DataFrame, options: SimpleNamespace):
        assert model_id == 'movies'
        assert df.count() == movies_df.count()

    driver.init(spark_session)
    driver.register_data_source_handler('connection', mock_input_handler)
    driver.register_output_handler('lake', mock_output_handler)
    driver.process_product(f'{os.path.dirname(os.path.abspath(__file__))}/assets/')
#     configure_spark(spark_session)
#     expected_df = spark_session.createDataFrame([('Jumanji(1995)', 3.0, 3),
#                                                  ('Heat (1995)', 4.0, 1),
#                                                  ], result_schema)
