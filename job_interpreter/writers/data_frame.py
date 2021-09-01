from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame


class DataFrameWriter:
    context: GlueContext

    def __init__(self, context: GlueContext):
        self.context = context

    def write(self, data_frame: DataFrame, options: dict):
        dynamic_df = DynamicFrame.fromDF(data_frame, self.context, 'dynamic_df')

        sink = self.context.getSink(
            connection_type='s3',
            path=options['location'],
            enableUpdateCatalog=True,
            partitionKeys=resolve_partition_keys(options)
        )
        sink.setFormat('json')
        sink.setCatalogInfo(catalogDatabase='customers', catalogTableName='person')
        sink.writeFrame(dynamic_df)


def resolve_partition_keys(options: dict):
    partition_key = options.get('partition_by')
    if partition_key:
        return [partition_key]
    return []
