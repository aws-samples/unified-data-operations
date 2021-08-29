from pyspark.sql.functions import concat, col, lit

from driver.common import get_data_set
from driver.task_executor import DataSet


def execute(inp_dfs: list[DataSet]):
    ds = get_data_set(inp_dfs, 'person')
    if ds:
        ds.df = ds.df.withColumn('full_name', concat(col('first_name'), lit(" "), col('last_name')))
    return inp_dfs
