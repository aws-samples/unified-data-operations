import botocore
from mypy_boto3_glue.type_defs import GetDatabasesResponseTypeDef, DatabaseTypeDef, GetTablesResponseTypeDef, \
    TableTypeDef, TableInputTypeDef, StorageDescriptorTypeDef, ColumnTypeDef, DatabaseInputTypeDef
from driver.aws import providers
from driver.aws.resolvers import resolve_table
from driver.task_executor import DataSet


def update_data_catalog(ds: DataSet):
    glue = providers.get_glue()

    def upsert_database():
        try:
            rsp: GetDatabasesResponseTypeDef = glue.get_database(ds.product_id)
            # todo: update database with changes
        except botocore.errorfactory.EntityNotFoundException as enf:
            # database does not exists yet
            print(
                f'Database {ds.product_id} does not exists in the data catalog. {str(enf)}. It is going to be created.')
            # todo: add permission model
            glue.create_database(DatabaseInputTypeDef(Name=ds.product_id, Descritpion=ds.product.description))

    def upsert_table():
        try:
            rsp: GetTablesResponseTypeDef = glue.get_table(DatabaseName=ds.product_id, Name=ds.model_id)
            # todo: update table
            glue.update_table(DatabaseName=ds.product_id, TableInput=resolve_table(ds))
        except botocore.errorfactory.EntityNotFoundException as enf:
            # table not found
            print(
                f'Table [{ds.model_id}] cannot be found in the database [{ds.product_id}] in Glue Data Catalog. {str(enf)}. It is going to be created.')
            glue.create_table(DatabaseName=ds.product_id, TableInput=resolve_table(ds))
        rsp: GetTablesResponseTypeDef = glue.get_table(DatabaseName=ds.product_id, Name=ds.model_id)
        # todo: update partitions
        # todo: register with lakeformation

    upsert_database()
    upsert_table()
