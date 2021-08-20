from .config import ConfigRepository


class TaskRepository:
    project: dict

    def __init__(self, config: ConfigRepository):
        self.project = config.get('./product.yml')

    def get(self, task_id: str):
        for task in self.project['product']['pipeline']['tasks']:
            if task['id'] == task_id:
                return task
