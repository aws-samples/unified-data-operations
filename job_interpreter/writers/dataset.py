from awsglue.context import GlueContext
from .data_frame import DataFrameWriter
from ..repositories import ModelRepository, ConfigRepository


class DatasetWriter:
    model_repository: ModelRepository
    data_frame_writer: DataFrameWriter

    def __init__(self, context: GlueContext, config_repository: ConfigRepository):
        self.data_frame_writer = DataFrameWriter(context)
        self.model_repository = ModelRepository(config_repository)

    def write(self, datasets: [dict]):
        for dataset in datasets:
            model = self.model_repository.get(dataset['model'])
            self.data_frame_writer.write(dataset['data_frame'], model['options'])
