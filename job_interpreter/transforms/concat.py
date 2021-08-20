from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws, col


class ConcatTransform:
    data_frame: DataFrame
    columns: List
    separator: str
    alias: str

    def __init__(self, data_frame: DataFrame, columns: List, separator: str = '', alias: str = 'value'):
        self.data_frame = data_frame
        self.columns = columns
        self.separator = separator
        self.alias = alias

    def transform(self) -> DataFrame:
        return self.data_frame\
            .withColumn(self.alias, concat_ws(self.separator, *self.columns))\
            .drop(*self.columns)
