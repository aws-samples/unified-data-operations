from .config import ConfigRepository


class ModelRepository:
    models: dict

    def __init__(self, config: ConfigRepository):
        self.models = config.get('./model.yml')

    def get(self, model_id):
        for model in self.models['models']:
            if model['id'] == model_id:
                return model
