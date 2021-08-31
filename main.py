import os
import argparse

from pyspark import SparkConf

import driver
import driver.aws_provider
from driver.io_handlers import connection_input_handler
from driver.processors import schema_checker, constraint_processor, transformer_processor
from driver.io_handlers import lake_input_handler, lake_output_handler, connection_input_handler


def init_aws(args):
    driver.aws_provider.init(profile=args.aws_profile, region=args.aws_region)


def init_system(product_path: str):
    driver.io_handlers.init(driver.aws_provider.connection_provider)
    conf = SparkConf()
    if args.local:
        jars = f"{os.path.dirname(os.path.abspath(__file__))}/spark_deps/postgresql-42.2.23.jar"
        conf.set("spark.jars", jars)

    driver.init(spark_config=conf)
    driver.register_data_source_handler('connection', connection_input_handler)
    driver.register_postprocessors(schema_checker, constraint_processor, transformer_processor)
    driver.register_output_handler('default', lake_output_handler)
    driver.register_output_handler('lake', lake_output_handler)
    driver.process_product(f'{os.path.dirname(os.path.abspath(__file__))}{product_path}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--JOB_NAME', help='the name of this pyspark job')
    parser.add_argument('--product_path', help='the data product definition folder')
    parser.add_argument('--aws_profile', help='the AWS profile to be used for connection')
    parser.add_argument('--aws_region', help='the AWS region to be used')
    parser.add_argument('--local', action='store_true', help='local development')
    args = parser.parse_args()
    print(f'PATH: {os.environ["PATH"]}')
    print(f'SPARK_HOME: {os.environ.get("SPARK_HOME")}')
    print(f'PYTHONPATH: {os.environ.get("PYTHONPATH")}')
    init_aws(args)
    init_system(f'{args.product_path}{os.path.sep if not args.product_path.endswith(os.path.sep) else None}')
