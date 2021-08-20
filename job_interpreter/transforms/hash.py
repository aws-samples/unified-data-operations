from pyspark.sql import DataFrame
from pyspark.sql.functions import udf, lit
from pyspark.sql.types import StringType

import hashlib


class HashTransform:
    data_frame: DataFrame
    column: str
    key: str

    def __init__(self, data_frame: DataFrame, column: str, key: str = None):
        self.data_frame = data_frame
        self.column = column
        self.key = key

    def transform(self) -> DataFrame:
        encrypt_udf = udf(encrypt, StringType())
        return self.data_frame.withColumn(self.column, encrypt_udf(lit(self.key), self.column))


def encrypt(key: str, value: object):
    if key:
        return hashlib.sha256(str(value).encode() + key.encode()).hexdigest()
    else:
        return hashlib.sha256(str(value).encode()).hexdigest()
