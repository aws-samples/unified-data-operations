import logging
import os
import argparse

from pyspark import SparkConf
import traceback
import driver
import driver.aws.providers
from driver.aws.providers import connection_provider, datalake_provider
from driver.io_handlers import connection_input_handler, lake_input_handler
from driver.processors import schema_checker, constraint_processor, transformer_processor, type_caster
from driver.io_handlers import lake_output_handler, connection_input_handler

logger = logging.getLogger(__name__)

def init_aws(args):
    profile = None
    region = None
    if hasattr(args, 'aws_profile'):
        profile = args.aws_profile
    if hasattr(args, 'aws_region'):
        region = args.aws_region
    driver.aws.providers.init(profile=profile, region=region)


def init_system(args):
    driver.io_handlers.init(connection_provider, datalake_provider)
    conf = SparkConf()
    if hasattr(args, 'aws_profile'):
        logger.info(f'Setting aws profile: {args.aws_profile}')
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
    driver.register_data_source_handler('model', lake_input_handler)
    driver.register_postprocessors(transformer_processor, constraint_processor, type_caster, schema_checker)
    driver.register_output_handler('default', lake_output_handler)
    driver.register_output_handler('lake', lake_output_handler)
    driver.process_product(args)


if __name__ == '__main__':
    try:
        logging.basicConfig(level=logging.INFO)
        parser = argparse.ArgumentParser(argument_default=argparse.SUPPRESS)
        parser.add_argument('--JOB_ID', help='the unique id of this Glue job')
        parser.add_argument('--JOB_RUN_ID', help='the unique id of this Glue job run')
        parser.add_argument('--JOB_NAME', help='the name of this Glue job')
        parser.add_argument('--job-bookmark-option', help="job-bookmark-disable if you don't want bookmarking")
        parser.add_argument('--TempDir', help='temporary results directory')
        parser.add_argument('--product_path', help='the data product definition folder')
        parser.add_argument('--aws_profile', help='the AWS profile to be used for connection')
        parser.add_argument('--aws_region', help='the AWS region to be used')
        parser.add_argument('--local', action='store_true', help='local development')
        parser.add_argument('--jars', help='extra jars to be added to the Spark context')
        parser.add_argument('--additional-python-modules', help='this is used by Glue, ignored by this code')
        parser.add_argument('--default_data_lake_bucket', help='Data Mesh output S3 bucket name', default=None)
        args = parser.parse_args()
        logger.info(f'PATH: {os.environ["PATH"]}')
        logger.info(f'SPARK_HOME: {os.environ.get("SPARK_HOME")}')
        logger.info(f'PYTHONPATH: {os.environ.get("PYTHONPATH")}')

        init_aws(args)
        if hasattr(args, "JOB_NAME") and not (hasattr(args, 'local') and args.local):
            import zipfile

            with zipfile.ZipFile(f'{os.path.dirname(os.path.abspath(__file__))}/{args.JOB_NAME}.zip', 'r') as zip_ref:
                zip_ref.extractall(f'{os.path.dirname(os.path.abspath(__file__))}/')
        init_system(args=args)
    except Exception as e:
        logging.error(str(e))
        traceback.print_exc()
        raise e
