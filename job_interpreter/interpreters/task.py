from pyspark.sql import DataFrame
from ..writers import DatasetWriter
from ..repositories import ModelRepository, TaskRepository, ConfigRepository
from ..mappers import DatasetMapper
from ..processor import ModelProcessor, DynamicProcessor


class TaskInterpreter:
    task_repository: TaskRepository
    dataset_mapper: DatasetMapper
    model_processor: ModelProcessor
    dataset_writer: DatasetWriter

    def __init__(self, context: GlueContext):
        config_repository = ConfigRepository(context.spark_session.sparkContext)
        self.task_repository = TaskRepository(config_repository)
        self.dataset_mapper = DatasetMapper(context, config_repository)
        self.model_processor = ModelProcessor(ModelRepository(config_repository))
        self.dataset_writer = DatasetWriter(context, config_repository)

    def interpret(self, task_id: str) -> DataFrame:
        task = self.task_repository.get(task_id)

        datasets = self.dataset_mapper.map(task)

        if task.get('logic'):
            datasets = DynamicProcessor(task.get('logic')).process(datasets)

        datasets = self.model_processor.process(datasets)

        self.dataset_writer.write(datasets)
