# How to reference custom Spark dependencies?

Sometimes you might need some third party libraries for your aggregation logic. These can be added by creating a
```requirements.txt``` file in the root of your Data Product folder. In the following example we show, how to use
Pydeequ (a third party analyzer and quality assurance library from Amazon):

```requirements.txt
pydeequ
```

Pydeequ (in this example) is the python binding to the Deequ Scala implementation, that needs additional non-python (
Scala or Java)
libraries to be added to the Spark cluster. This can be added via a ```config.ini``` file (also stored in the root of
the data product).

```properties
[spark jars]
spark.jars.packages=com.amazon.deequ:deequ:1.2.2-spark-3.0
spark.jars.excludes=net.sourceforge.f2j:arpack_combined_all
```

Once the pre-requisites are there, you can start usin the new library in your custom logic:

```python
from pyspark.sql.functions import concat, col, lit
from driver.common import find_dataset_by_id
from driver.task_executor import DataSet
from typing import List
from pyspark.sql import SparkSession, Row
from pydeequ.analyzers import *


def execute(inp_dfs: List[DataSet], spark_session: SparkSession):
    ds = find_dataset_by_id(inp_dfs, 'sample_product.sample_model')
    ds.df = ds.df.withColumn('full_name', concat(col('first_name'), lit(' '), col('last_name')))

    analysis_result = AnalysisRunner(spark_session)
    .onData(ds.df)
    .addAnalyzer(Size())
    .addAnalyzer(Completeness("b"))
    .run()


analysis_result_df = AnalyzerContext.successMetricsAsDataFrame(spark_session, analysis_result)

ds_model = DataSet(id='sample_model', df=ds.df)
ds_analysis = DataSet(id='model_analysis', df=analysis_result_df)
return [ds_model, ds_analysis]
```

Additionally you can create a custom initialisation file, called ```init_hook.py``` in the root folder of the data
product. This file will give you control over the Spark environment and the data product processor envrioenment as well.

```python
from typing import List, Dict
from pyspark import SparkConf
from driver.task_executor import DataSet


def enrich_spark_conf(conf: SparkConf) -> SparkConf:
    conf.set("spark.sql.warehouse.dir", "some warehouse location")
    return conf


def add_pre_processors() -> List[callable]:
    def my_custom_pre_processor(data_set: DataSet) -> DataSet:
        return data_set.df.filter(...)

    return [my_custom_pre_processor]


def add_post_processors() -> List[callable]:
    def my_custom_post_processor(data_set: DataSet) -> DataSet:
        return data_set.df.filter(...)

    return [my_custom_post_processor]
```

**Please note:** all of the above methods are optional. The Spark configuration can also be influenced by the use of the
ini file.

#### Preparing your unit test to work with Pyspark custom configurations

Create a file ```pytest.ini``` and add Spark options:

```properties
[pytest]
spark_options=
spark.jars.packages:com.amazon.deequ:deequ:1.2.2-spark-3.0
spark.jars.excludes:net.sourceforge.f2j:arpack_combined_all
```