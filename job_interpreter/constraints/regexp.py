from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from .validation_result import ValidationResult


class RegExpConstraint:
    data_frame: DataFrame
    column: str
    regexp: str

    def __init__(self, data_frame: DataFrame, column: str, regexp: str):
        self.data_frame = data_frame
        self.column = column
        self.regexp = regexp

    def validate(self) -> ValidationResult:
        count = self.data_frame \
            .select(col(self.column)) \
            .count()
        match_count = self.data_frame \
            .where(col(self.column).rlike(self.regexp))\
            .count()

        return ValidationResult('regexp', self.column, match_count == count, count, match_count)
