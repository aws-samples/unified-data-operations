from awsglue.context import GlueContext

from ..providers import DataFrameProvider
from ..repositories import ConfigRepository


class DatasetMapper:
    data_frame_provider: DataFrameProvider

    def __init__(self, context: GlueContext, config_repository: ConfigRepository):
        self.data_frame_provider = DataFrameProvider(context, config_repository)

    def map(self, task: dict):
        context = []

        for input_dict in task['input']:
            context.append({
                'input': input_dict['id'],
                'model': input_dict['model'],
                'data_frame': self.data_frame_provider.get(input_dict)
            })
        return context
