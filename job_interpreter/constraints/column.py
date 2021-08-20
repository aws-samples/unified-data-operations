from typing import List
from ..constraints import ConstraintRegistry
from pyspark.sql import DataFrame


def get_option(constraint: dict, option_id, default_value: object):
    if 'options' in constraint:
        options = constraint['options']
        return options.get(option_id, default_value)
    return default_value


class ConstraintColumnMapper:
    registry: ConstraintRegistry
    data_frame: DataFrame

    def __init__(self, registry: ConstraintRegistry, data_frame: DataFrame):
        self.registry = registry
        self.data_frame = data_frame

    def map(self, column_id: str, column: dict) -> List:
        constraints = [self.registry.get('exist')(self.data_frame, column_id)]

        constraints_dict = column.get('constraints', {})

        for constraint_dict in constraints_dict:
            constraint_type = constraint_dict.get('type')
            constraint = self.registry.get(constraint_type)

            if constraint_type == 'not_null':
                threshold = get_option(constraint_dict, 'threshold', 0.0)
                constraints.append(constraint(self.data_frame, column_id, threshold))
            if constraint_type == 'regexp':
                regex = get_option(constraint_dict, 'regex', '.*')
                constraints.append(constraint(self.data_frame, column_id, regex))
            if constraint_type == 'distinct':
                constraints.append(constraint(self.data_frame, column_id))

        return constraints
