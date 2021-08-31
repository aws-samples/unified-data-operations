import sys
import traceback
from types import SimpleNamespace
import findspark
import yaml
from pyspark.sql import SparkSession
from driver import task_executor

__SPARK__: SparkSession = None


def get_spark():
    if __SPARK__:
        return __SPARK__
    else:
        raise RuntimeError('Spark Session is not created yet. Call init() first.')


def get_or_create_session(config=None) -> SparkSession:  # pragma: no cover
    """Build spark session for jobs running on cluster."""
    spark = SparkSession.builder.appName(__name__) \
        .config(conf=config) \
        .getOrCreate()

    return spark


def init(spark_session=None, spark_config=None):
    global __SPARK__
    if not spark_session:
        findspark.init()
        __SPARK__ = get_or_create_session(spark_config)
    else:
        __SPARK__ = spark_session


def load_yaml_into_object(file_type, cfg_file_prefix: str = None) -> SimpleNamespace:
    def parse(d: dict):
        x = SimpleNamespace()
        for k, v in d.items():
            if isinstance(v, dict):
                setattr(x, k, parse(v))
            elif isinstance(v, list):
                object_list = list()
                for e in v:
                    object_list.append(parse(e))
                setattr(x, k, object_list)
            else:
                setattr(x, k, v)
        return x

    path = f'{cfg_file_prefix}{file_type}' if cfg_file_prefix else file_type
    with open(fr'{path}') as file:
        dict_val = yaml.load(file, Loader=yaml.FullLoader)
        return parse(dict_val)


def execute_tasks(tasks: list, models: list):
    for task in tasks:
        task_executor.execute(task, models)


def process_product(product_path: str):
    try:
        product = load_yaml_into_object('product.yml', product_path).product
        models = load_yaml_into_object('model.yml', product_path)
        execute_tasks(product.pipeline.tasks, models)
    except Exception as e:
        traceback.print_exc()
        print(f"Couldn't execute job due to >> {type(e).__name__}: {str(e)}")
        sys.exit(-1)
        raise e
