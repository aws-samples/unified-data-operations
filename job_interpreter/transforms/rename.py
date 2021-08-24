from pyspark.sql import DataFrame


class RenameTransform:
    data_frame: DataFrame
    column_id: str
    new_column_id: str

    def __init__(self, data_frame: DataFrame, column_id: str, new_column_id: str):
        self.data_frame = data_frame
        self.column_id = column_id
        self.new_column_id = new_column_id

    def transform(self) -> DataFrame:
        return self.data_frame.withColumnRenamed(self.column_id, self.new_column_id)
