from boto3.session import Session
from driver.task_executor import DataSet
from .database import DatabaseService
from .table import TableService


class CatalogService:
    database_service: DatabaseService
    table_service: TableService

    def __init__(self, session: Session):
        self.database_service = DatabaseService(session)
        self.table_service = TableService(session)

    def drain_database(self, database_name: str):
        database = self.database_service.get_database(database_name)

        if database:
            self.table_service.delete_by_database_id(database_name)

    def update_database(self, database_name: str, table_name: str, data_set: DataSet):
        self.database_service.get_or_create_database(database_name)

        self.table_service.recreate_table(database_name, table_name, data_set)
