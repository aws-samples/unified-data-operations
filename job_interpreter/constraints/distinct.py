from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from .validation_result import ValidationResult


class DistinctConstraint:
    data_frame: DataFrame
    column: str

    def __init__(self, data_frame: DataFrame, column: str):
        self.data_frame = data_frame
        self.column = column

    def validate(self) -> ValidationResult:
        count = self.data_frame \
            .select(col(self.column)) \
            .count()
        distinct_count = self.data_frame \
            .select(col(self.column))\
            .distinct()\
            .count()

        return ValidationResult('distinct', self.column, distinct_count == count, count, distinct_count)
