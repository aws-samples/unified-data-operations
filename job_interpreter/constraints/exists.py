from pyspark.sql import DataFrame
from .validation_result import ValidationResult


class ExistsConstraint:
    data_frame: DataFrame
    column: str

    def __init__(self, data_frame: DataFrame, column: str):
        self.data_frame = data_frame
        self.column = column

    def validate(self) -> ValidationResult:
        if self.column in self.data_frame.columns:
            return ValidationResult('exists', self.column, True, self.column, self.column)
        else:
            return ValidationResult('exists', self.column, False, self.column, None)
