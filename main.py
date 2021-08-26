import os
import sys
import traceback

import yaml
import argparse

from pyspark import SparkContext
from pyspark.sql import SparkSession

from job_interpreter import executor

from awsglue.utils import getResolvedOptions
args = getResolvedOptions(sys.argv, ['JOB_NAME'])


def get_or_create_session(config=None) -> SparkSession:  # pragma: no cover
    """Build spark session for jobs running on cluster."""

    spark = SparkSession.builder.appName(__name__).getOrCreate()

    for key, value in (config or {}).items():
        spark.conf.set(key, value)

    return spark


def load_yaml(file_type, cfg_file_prefix: str = None) -> dict:
    path = f'{cfg_file_prefix}{file_type}' if cfg_file_prefix else file_type
    with open(fr'{path}') as file:
        return yaml.load(file, Loader=yaml.FullLoader)


def execute_tasks(spark_context: SparkContext, cfg_file_prefix: str = None):
    tasks = load_yaml('product.yml', cfg_file_prefix).get('product').get('pipeline').get('tasks')
    models = load_yaml('model.yml', cfg_file_prefix)
    for task in tasks:
        executor.execute_task(task)


def main(spark_config=None):
    try:
        spark_session = get_or_create_session()
        # glueContext = GlueContext(spark_session.sparkContext)
        # job = Job(glueContext)
        # job.init(args['JOB_NAME'], args)

        execute_tasks(spark_session.sparkContext, f'{os.path.dirname(os.path.abspath(__file__))}/tests/assets/')
        # job.commit()
    except Exception as e:
        traceback.print_exc()
        print(f"Couldn't execute job due to >> {str(e)}")
        sys.exit(-1)
        raise e


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    main()
