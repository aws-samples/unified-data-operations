from boto3.session import Session
from typing import List
from mypy_boto3_glue.client import GlueClient
from mypy_boto3_glue.type_defs import GetDatabasesResponseTypeDef, DatabaseTypeDef, GetTablesResponseTypeDef, TableTypeDef, TableInputTypeDef, StorageDescriptorTypeDef, ColumnTypeDef
from driver.task_executor import DataSet


class DatabaseService:
    client: GlueClient

    def __init__(self, session: Session):
        self.client = session.client('glue')

    def get_or_create_database(self, database_name: str):
        database = self.get_database(database_name)

        if database:
            return database

        return self.create_database(DatabaseTypeDef(Name=database_name))

    def create_database(self, database: DatabaseTypeDef):
         self.client.create_database(DatabaseInput=database)
         return self.client.get_database(Name=database.get('Name'))

    def get_database(self, database_name: str):
        get_databases_response: GetDatabasesResponseTypeDef = self.client.get_databases()
        databases: List[DatabaseTypeDef] = get_databases_response.get('DatabaseList')

        for database in databases:
            if database.get('Name') == database_name:
                return database
