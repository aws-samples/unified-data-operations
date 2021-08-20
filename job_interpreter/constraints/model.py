from typing import List
from pyspark.sql import DataFrame
from ..constraints import ConstraintRegistry
from .column import ConstraintColumnMapper


class ConstraintModelMapper:
    column_mapper: ConstraintColumnMapper

    def __init__(self, data_frame: DataFrame):
        self.column_mapper = ConstraintColumnMapper(ConstraintRegistry(), data_frame)

    def map(self, model: dict) -> List:
        constraints = []
        columns = model.get('columns')
        for column in columns:
            constraints.extend(self.column_mapper.map(column['id'], column))

        return constraints
