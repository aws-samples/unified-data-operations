from .skip import SkipTransform
from .hash import HashTransform
from pyspark.sql import DataFrame


def get_option(value: dict, option_id, default_value: object = None):
    if 'options' in value:
        options = value['options']
        return options.get(option_id, default_value)
    return default_value


class Transformer:
    data_frame: DataFrame

    def __init__(self, data_frame: DataFrame):
        self.data_frame = data_frame

    def transform(self, model: dict) -> DataFrame:
        data_frame = self.data_frame
        columns = model['columns']

        for column in columns:
            column_id = column['id']
            transforms_dict = column.get('transform', {})

            for transform_dict in transforms_dict:
                transform_type = transform_dict.get('type')
                if transform_type == 'skip':
                    data_frame = SkipTransform(data_frame, column_id).transform()
                if transform_type == 'encrypt':
                    key = get_option(transform_dict, 'key', None)
                    data_frame = HashTransform(data_frame, column_id, key).transform()

        return data_frame
