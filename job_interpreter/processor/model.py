from ..repositories import ModelRepository
from ..constraints import ConstraintModelMapper, ConstraintValidator, to_string
from ..transforms import Transformer


class ModelProcessor:
    model_repository: ModelRepository

    def __init__(self, model_repository: ModelRepository):
        self.model_repository = model_repository

    def process(self, datasets: [dict]) -> [dict]:
        result_datasets = []

        for dataset in datasets:
            self.process_input(dataset)

        return result_datasets

    def process_input(self, dataset: dict) -> dict:
        model = self.model_repository.get(dataset['model'])
        data_frame = dataset['data_frame']

        constraints = ConstraintModelMapper(data_frame).map(model)

        validation_errors = ConstraintValidator(constraints).validate()

        if validation_errors:
            raise RuntimeError(to_string(validation_errors))

        data_frame = Transformer(data_frame).transform(model)

        return {
            'data_frame': data_frame,
            'model': dataset['model'],
        }
