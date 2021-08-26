from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


class BucketTransform:
    data_frame: DataFrame
    column: str
    bins: []

    def __init__(self, data_frame: DataFrame, column: str, bins: []):
        self.data_frame = data_frame
        self.column = column
        self.bins = bins
        self.bins.sort(reverse=True)

    def transform(self) -> DataFrame:
        bins = self.bins
        cut_udf = udf(lambda l: cut(bins, l), StringType())
        return self.data_frame.withColumn(self.column, cut_udf(self.column))


def cut(bins: [], value: int):
    last_bin = 'inf'

    for bin in bins:
        if value > bin:
            return '({0},{1}]'.format(bin, last_bin)
        last_bin = bin

    return '({0},{1}]'.format('-inf', last_bin)