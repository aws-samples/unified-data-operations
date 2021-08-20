from awsglue.context import GlueContext
from ..repositories import ConnectionRepository
from ..repositories import ConfigRepository


class DataFrameProvider:
    context: GlueContext
    connection_repository: ConnectionRepository
    data_frames: dict

    def __init__(self, context: GlueContext, config_repository: ConfigRepository):
        self.context = context
        self.connection_repository = ConnectionRepository(config_repository)
        self.data_frames = {}

    def get(self, input_dict):
        name = input_dict['id']
        data_frame = self.data_frames[name]

        if not data_frame:
            data_frame = self.create(input_dict)
            self.data_frames[name] = data_frame

        return data_frame

    def create(self, input_dict: dict):
        connection = self.connection_repository.get(input_dict['connection'])

        database = connection['db_name']
        table_name = input_dict['table']

        return self.context.create_dynamic_frame\
            .from_catalog(database=database, table_name=table_name) \
            .toDF()
