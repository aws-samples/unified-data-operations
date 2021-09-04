from boto3.session import Session
from typing import List
from mypy_boto3_glue.client import GlueClient
from mypy_boto3_glue.type_defs import GetTablesResponseTypeDef, TableTypeDef
from driver.task_executor import DataSet
from driver.aws.resolvers import resolve_table


class TableService:
    client: GlueClient

    def __init__(self, session: Session):
        self.client = session.client('glue')

    def delete_by_database_id(self, database_name: str):
        tables = self.get_tables(database_name)

        for table in tables:
            self.delete_table(database_name, table.get('Name'))

    def recreate_table(self, database_name:str, table_name: str, data_set: DataSet):
        table = self.get_table(database_name, table_name)

        if table:
            self.delete_table(database_name, table_name)

        return self.create_table(database_name, table_name, data_set)

    def create_table(self, database_name:str, table_name:str, data_set: DataSet):
        self.client.create_table(DatabaseName=database_name, TableInput=resolve_table(table_name, data_set))
        return self.client.get_table(DatabaseName=database_name, Name=table_name)

    def delete_table(self, database_name: str, table_name: str):
        self.client.delete_table(DatabaseName=database_name, Name=table_name)

    def get_tables(self, database_name: str) -> List[TableTypeDef]:
        get_tables_response: GetTablesResponseTypeDef = self.client.get_tables(DatabaseName=database_name)
        return get_tables_response.get('TableList')

    def get_table(self, database_name: str, table_name: str):
        for table in self.get_tables(database_name):
            if table.get('Name') == table_name:
                return table
