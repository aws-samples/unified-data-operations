from typing import List

from pyspark.sql.functions import col, first, sum as summe

from driver.common import get_data_set
from driver.core import DataSet


def execute(inp_dfs: List[DataSet]):
    person = get_data_set(inp_dfs, 'person').df.alias('person')
    sales = get_data_set(inp_dfs, 'sales').df.alias('sales')

    person = person.join(sales, on=person.id == sales.ticketholder_id).select(
        col("person.id").alias('customer_id'),
        col('person.full_name'),
        col('person.age'),
        col("sales.ticket_price")
    )

    person = person.groupBy("customer_id").agg(
        first("person.full_name").alias('customer_name'),
        first("person.age").alias('age'),
        summe('sales.ticket_price').alias('revenue'),
    )

    output_ds = DataSet(model_id='revenue', df=person, input_id=None, model=None, product=None)
    return [output_ds]
