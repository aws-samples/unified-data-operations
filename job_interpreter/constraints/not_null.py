from pyspark.sql import DataFrame
from pyspark.sql.functions import when, count, col, lit
from .validation_result import ValidationResult


class NotNullConstraint:
    data_frame: DataFrame
    column: str
    threshold: float

    def __init__(self, data_frame: DataFrame, column: str, threshold: float = 0.0):
        self.data_frame = data_frame
        self.column = column
        self.threshold = threshold

    def validate(self) -> ValidationResult:
        null_value_ratio = self.data_frame \
            .select(count(when(col(self.column).isNull(), True))/count(lit(1)).alias('count')) \
            .first()[0]

        return ValidationResult('not_null', self.column, null_value_ratio <= self.threshold, self.threshold, null_value_ratio)
