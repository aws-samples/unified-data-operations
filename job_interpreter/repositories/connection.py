from .config import ConfigRepository


class ConnectionRepository:
    project: dict

    def __init__(self, config: ConfigRepository):
        self.project = config.get('./product.yml')

    def get(self, connection_id):
        for connection in self.project['product']['connections']:
            if connection['id'] == connection_id:
                return connection
