from pyspark.sql import DataFrame


class SkipTransform:
    data_frame: DataFrame
    column: str

    def __init__(self, data_frame: DataFrame, column: str):
        self.data_frame = data_frame
        self.column = column

    def transform(self) -> DataFrame:
        return self.data_frame.drop(self.column)
