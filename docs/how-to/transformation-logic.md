# How to write and test custom aggregation logic?

Each custom aggregation logic has the same anatomy: it receives a list of input DataSets (that contains the Spark
DataFrame)
and must produce at least one output DataSet with a Spark DataFrame inside. Everything in between is standard Python and
PySpark.

The example below receives one DataSet with the ID ```person_raw```, adds a new timestamp column if
the ```create_timestamp```
property was defined in the ```product.yml```'s pipeline > tasks > logic > parameters section and concatenates the
first_name and last_names columns into a full_name column. The very same DataFrame is packaged into two different
DataSets, with two different models referred to in the id property, so that the processor can do some post-processing on
the dataframes, that are defined in those models.

```python
def execute(inp_dfs: List[DataSet], spark_session: SparkSession, create_timestamp=False):
    ds = find_dataset_by_id(inp_dfs, 'person_raw')

    if create_timestamp:
        timestamp = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        ds.df = ds.df.withColumn('time', unix_timestamp(lit(timestamp), 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))

    df = ds.df.withColumn('full_name', concat(col('first_name'), lit(' '), col('last_name')))

    ds_pub = DataSet(id='person_pub', df=df)
    ds_pii = DataSet(id='person_pii', df=df)

    return [ds_pub, ds_pii]
```

In the example above, it is mandatory to provide the ```inp_dfs``` and the ```spark_session``` parameters, because these
are injected by the task executor.

The DataSet class provides access to the Spark Data Frame, as well to the model and the product metadata structure.

```python
@dataclass
class DataSet:
    id: str
    df: DataFrame
    model: SimpleNamespace = None
    product: DataProduct = None
```

These can be referenced in each custom aggregation task code.

Your custom aggregation logic is parametrised from the ```product.yml``` file's ```tasks``` section:

```yaml
  logic:
    module: tasks.custom_business_logic
    parameters:
      create_timestamp: false
```

## Testing

We recommend using the ```pytest``` framework for writing unit tests for your custom logic.

### 1) Create a virtual environment in root folder

```commandline
python3 -m venv .venv
source .venv/bin/activate
```

### 2) Install data-product-processor
```commandline
pip install data-product-processor
```

### 3) Install python dependencies for test execution

Create a ```requirements-test.txt``` file in the root folder of the data product with the following content:

```text
pyspark
pyspark-stubs
pytest-spark
pytest-mock
pytest-helpers-namespace
pytest-env
pytest-cov
pytest
```

Install them.
```commandline
pip install -r requirements-test.txt
```

### 4) Add tests

Create a ```tests``` folder in your data product folder.

```commandline
mkdir tests
touch tests/__init__.py
```
Create a test configuration file called ```test_config.py```
with [fixtures](https://docs.pytest.org/en/6.2.x/fixture.html) (reusable, support functionality injected into your tests
by the pytest framework).

```python

from types import SimpleNamespace
from pyspark.sql import DataFrame
from pytest import fixture
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    IntegerType
)

DEFAULT_BUCKET = 's3://test-bucket'


@fixture
def app_args() -> SimpleNamespace:
    args = SimpleNamespace()
    setattr(args, 'default_data_lake_bucket', DEFAULT_BUCKET)
    return args


@fixture(scope='module')
def person_schema() -> StructType:
    return StructType([
        StructField('id', IntegerType(), False),
        StructField('first_name', StringType(), True),
        StructField('last_name', StringType(), True),
        StructField('age', IntegerType(), True),
        StructField('city', StringType(), True),
        StructField('gender', StringType(), True),
    ])


@fixture(scope='module')
def person_df(spark_session, person_schema) -> DataFrame:
    return spark_session.createDataFrame([(1, "John", "Doe", 25, "Berlin", "male"),
                                          (2, "Jane", "Doe", 41, "Berlin", "female"),
                                          (3, "Maxx", "Mustermann", 30, "Berlin", "male")
                                          ], person_schema)
```

Next write your test function for your custom business logic in the ```test_custom_business_logic.py``` file:

```python
from pyspark.sql import DataFrame


def test_custom_logic(spark_session, person_df: DataFrame):
    data_source = DataSet(id='some_schema.some_table', df=person_df)
    results: List[DataSet] = tasks.custom_business_logic.execute([data_source], spark_session)
    for dataset in results:
        assert dataset.id == 'transformed_data_set'
        assert dataset.df.count() == person_df.count()
        dataset.df.show()
        dataset.df.describe()
```

You might want to run an end-to-end test, by wiring together the minimal structure of the data product processor:

```python
from types import SimpleNamespace
from driver import DataSet
from driver.processors import schema_checker, constraint_processor, transformer_processor


def test_end_to_end(spark_session, spark_context, person_df: DataFrame, app_args):
    product_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')

    def mock_input_handler(input_definition: SimpleNamespace):
        dfs = {"source_id": person_df}
        return dfs.get(input_definition.table)

    def mock_output_handler(dataset: DataSet):
        assert dataset.id == 'transformed_data_set'
        assert dataset.df.count() == person_df.count()
        dataset.df.show()
        dataset.df.describe()

    driver.init(spark_session)
    driver.register_data_source_handler('connection', mock_input_handler)
    driver.register_postprocessors(transformer_processor, schema_checker, constraint_processor)
    driver.register_output_handler('default', mock_output_handler)
    driver.register_output_handler('lake', mock_output_handler)
    driver.process_product(app_args, product_folder)
```

You can run your tests from your favourite editor (eg. Pycharm) or using the ```pytest``` command line.
