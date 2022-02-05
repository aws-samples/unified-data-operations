import logging

import sys, os
import traceback

from typing import List
from types import SimpleNamespace
from pyspark.sql import SparkSession
from driver import task_executor
from .aws import providers
from deprecated import CatalogService
from .core import DataProduct
from .util import compile_models, compile_product

__SPARK__: SparkSession = None
logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    if __SPARK__:
        return __SPARK__
    else:
        raise RuntimeError('Spark Session is not created yet. Call init() first.')


def get_or_create_session(config=None) -> SparkSession:  # pragma: no cover
    """Build spark session for jobs running on cluster."""
    spark = SparkSession.builder.appName(__name__) \
        .config(conf=config) \
        .enableHiveSupport() \
        .getOrCreate()

    return spark


def init(spark_session=None, spark_config=None):
    global __SPARK__
    if not spark_session:
        __SPARK__ = get_or_create_session(spark_config)
    else:
        __SPARK__ = spark_session
    # sc  = __SPARK__.sparkContext
    # sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")


def execute_tasks(product: SimpleNamespace, tasks: list, models: List[SimpleNamespace], product_path: str):
    session = providers.get_session()
    if session:
        CatalogService(session).drain_database(product.id) #todo: check this implementation here if needed

    for task in tasks:
        task_executor.execute(product, task, models, product_path)


def process_product(args):
    try:
        # script_folder = os.path.dirname(os.path.abspath(__file__))
        # path = f'{config_file_path_prefix}{file_type}' if config_file_path_prefix else file_type
        rel_product_path = os.path.join(args.product_path, '') if hasattr(args, 'product_path') else os.path.join('./',
                                                                                                                  '')
        abs_product_path = os.path.join(os.path.abspath(rel_product_path), '')
        product = compile_product(abs_product_path, args)
        models = compile_models(abs_product_path, product)
        execute_tasks(product, product.pipeline.tasks, models, abs_product_path)
    except Exception as e:
        traceback.print_exc()
        logger.error(f"Couldn't execute job due to >> {type(e).__name__}: {str(e)}")
        sys.exit(-1)
        # raise e
