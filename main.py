import os
import argparse

from pyspark import SparkConf
import traceback
import driver
import driver.aws.providers
from driver.io_handlers import connection_input_handler
from driver.processors import schema_checker, constraint_processor, transformer_processor
from driver.io_handlers import lake_output_handler, connection_input_handler


def init_aws(args):
    profile = None
    region = None
    if hasattr(args, 'aws_profile'):
        profile = args.aws_profile
    if hasattr(args, 'aws_region'):
        region = args.aws_region
    driver.aws.providers.init(profile=profile, region=region)


def init_system(product_def_path: str):
    driver.io_handlers.init(driver.aws.providers.connection_provider)
    conf = SparkConf()
    if hasattr(args, 'aws_profile'):
        print(f'Setting aws profile: {args.aws_profile}')
        os.environ["AWS_PROFILE"] = args.aws_profile
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    # conf.set("spark.sql.warehouse.dir", warehouse_location)
    if hasattr(args, 'local') and args.local:
        deps_path = f'{os.path.dirname(os.path.abspath(__file__))}/spark_deps'
        pgres_driver_jars = f'{deps_path}/postgresql-42.2.23.jar'
        local_jars = [pgres_driver_jars]
        if args.jars:
            local_jars.extend([f'{deps_path}/{j}' for j in args.jars.strip().split(',')])
        jars = ','.join(local_jars)
        conf.set("spark.jars", jars)
    driver.init(spark_config=conf)
    driver.register_data_source_handler('connection', connection_input_handler)
    driver.register_postprocessors(schema_checker, constraint_processor, transformer_processor)
    driver.register_output_handler('default', lake_output_handler)
    driver.register_output_handler('lake', lake_output_handler)
    driver.process_product(f'{os.path.dirname(os.path.abspath(__file__))}/{product_def_path}')


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(argument_default=argparse.SUPPRESS)
        parser.add_argument('--JOB_ID', help='the unique id of this Glue job')
        parser.add_argument('--JOB_RUN_ID', help='the unique id of this Glue job run')
        parser.add_argument('--JOB_NAME', help='the name of this Glue job')
        parser.add_argument('--job-bookmark-option', help="job-bookmark-disable if you don't want bookmarking")
        parser.add_argument('--TempDir', help='tempoarary results directory')
        parser.add_argument('--product_path', help='the data product definition folder')
        parser.add_argument('--aws_profile', help='the AWS profile to be used for connection')
        parser.add_argument('--aws_region', help='the AWS region to be used')
        parser.add_argument('--local', action='store_true', help='local development')
        parser.add_argument('--jars', help='extra jars to be added to the Spark context')
        parser.add_argument('--additional-python-modules', help='this is used by Glue, ignored by this code')
        args = parser.parse_args()
        print(f'PATH: {os.environ["PATH"]}')
        print(f'SPARK_HOME: {os.environ.get("SPARK_HOME")}')
        print(f'PYTHONPATH: {os.environ.get("PYTHONPATH")}')

        init_aws(args)
        if hasattr(args, "JOB_NAME") and not args.local:
            import zipfile
            with zipfile.ZipFile(f'{os.path.dirname(os.path.abspath(__file__))}/{args.JOB_NAME}.zip', 'r') as zip_ref:
                zip_ref.extractall(f'{os.path.dirname(os.path.abspath(__file__))}/')

        product_path = args.product_path if hasattr(args, 'product_path') else './'
        init_system(f'{product_path}{os.path.sep if not product_path.endswith(os.path.sep) else ""}')
    except Exception as e:
        print(str(e))
        traceback.print_exc()
