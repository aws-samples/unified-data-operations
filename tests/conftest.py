from pytest import fixture
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType,
    LongType,
    DoubleType
)


@fixture(scope='module')
def movie_schema():
    return StructType([
        StructField('movieId', IntegerType(), True),
        StructField('title', StringType(), True),
        StructField('genres', StringType(), True)
    ])


@fixture(scope='module')
def ratings_schema():
    return StructType([
        StructField('userId', IntegerType(), True),
        StructField('movieId', IntegerType(), True),
        StructField('rating', IntegerType(), True),
        StructField('timestamp', LongType(), True)
    ])


@fixture(scope='module')
def result_schema():
    return StructType([
        StructField('title', StringType(), True),
        StructField('weight_avg', DoubleType(), True),
        StructField('num_votes', IntegerType(), True)
    ])


@fixture(scope='module')
def movies_df(spark_session, movie_schema):
    return spark_session.createDataFrame([(1, 'Jumanji(1995)', 'Adventure | Children | Fantasy'),
                                          (2, 'Heat (1995)', 'Action|Crime|Thriller')],
                                         movie_schema)


@fixture(scope='module')
def ratings_df(spark_session, ratings_schema):
    return spark_session.createDataFrame([(1, 1, 4, 1256677221),
                                          (2, 1, 4, 1256677222),
                                          (3, 1, 1, 1256677222),
                                          (4, 2, 4, 1256677222)
                                          ], ratings_schema)


@fixture(scope='module')
def person_schema():
    return StructType([
        StructField('id', IntegerType(), False),
        StructField('first_name', StringType(), True),
        StructField('last_name', StringType(), True),
        StructField('age', IntegerType(), True),
        StructField('city', StringType(), True),
        StructField('gender', StringType(), True),
    ])


@fixture(scope='module')
def person_df(spark_session, person_schema):
    return spark_session.createDataFrame([(1, "John", "Doe", 25, "Berlin", "Male"),
                                          (2, "Jane", "Doe", 41, "Berlin", "Female"),
                                          (3, "Maxx", "Mustermann", 30, "Berlin", "Male")
                                          ], person_schema)
