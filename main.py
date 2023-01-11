# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import configparser
import importlib
import logging
import os
import argparse
import sys

from pyspark import SparkConf
import traceback
import driver
import driver.aws.providers
from driver.aws.providers import connection_provider, datalake_provider
from driver.io_handlers import connection_input_handler, lake_input_handler, file_input_handler
from driver.processors import schema_checker, constraint_processor, transformer_processor, type_caster, razor
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


def build_spark_configuration(args, config: configparser.RawConfigParser, custom_hook: callable = None):
    conf = SparkConf()
    if hasattr(args, 'aws_profile'):
        logger.info(f'Setting aws profile: {args.aws_profile}')
        os.environ["AWS_PROFILE"] = args.aws_profile
        conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    if hasattr(args, 'local') and args.local:
        deps_path = os.path.join(os.getcwd(), 'spark_deps')
        local_jars = [file for file in os.listdir(deps_path) if file.endswith('.jar')]
        if hasattr(args, 'jars'):
            local_jars.extend([f'{deps_path}/{j}' for j in args.jars.strip().split(',')])
        jars = ','.join([os.path.join(deps_path, j) for j in local_jars])
        conf.set("spark.jars", jars)
    if config:
        spark_jars = 'spark jars'
        if spark_jars in config.sections():
            for k, v in config.items(spark_jars):
                conf.set(k, v)
    return custom_hook.enrich_spark_conf(conf) if custom_hook and hasattr(custom_hook, 'enrich_spark_conf') else conf


def read_config(product_path: str) -> configparser.RawConfigParser:
    config_path = os.path.join(product_path, 'config.ini')
    if os.path.isfile(config_path):
        config = configparser.ConfigParser()
        config.read(config_path)
        return config
    else:
        return None


def get_custom_hook(product_path: str) -> callable:
    hook_module_name = 'init_hook'
    hook_file = f'{hook_module_name}.py'
    hook_file_name = os.path.join(product_path, hook_file)
    if os.path.exists(hook_file_name):
        sys.path.append(product_path)
        logger.info(f'executing custom hooks: {hook_file_name}')
        module = importlib.import_module(hook_module_name)
        sys.modules[hook_module_name] = module
        return module
    else:
        return None


def init_system(args):
    driver.io_handlers.init(connection_provider, datalake_provider)
    rel_product_path = os.path.join(args.product_path, '') if hasattr(args, 'product_path') else os.path.join('./', '')
    product_path = os.path.join(os.path.abspath(rel_product_path), '')
    config = read_config(product_path)
    custom_hook = get_custom_hook(product_path)
    driver.init(spark_config=build_spark_configuration(args, config, custom_hook))
    driver.install_dependencies(product_path)
    driver.register_data_source_handler('connection', connection_input_handler)
    driver.register_data_source_handler('model', lake_input_handler)
    driver.register_data_source_handler('file', file_input_handler)
    driver.register_postprocessors(transformer_processor, razor, constraint_processor, type_caster, schema_checker)
    driver.register_output_handler('default', lake_output_handler)
    driver.register_output_handler('lake', lake_output_handler)
    if custom_hook:
        if hasattr(custom_hook, 'add_post_processors'):
            driver.register_postprocessors(*custom_hook.add_post_processors())
        if hasattr(custom_hook, 'add_pre_processors'):
            driver.register_preprocessors(*custom_hook.add_pre_processors())
        # if hasattr(custom_hook, 'add_transformers'):
        #     driver.add_transformers(custom_hook.add_transformers())
        # todo: the transformer dict is not used, the processor built-in transformers are the only ones looked up now
    driver.process_product(args, product_path)


def main():
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
        args, unknown = parser.parse_known_args()
        logger.info(f"KNOWN_ARGS: {args}")
        logger.info(f"UNKNOWN_ARGS: {unknown}")
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
        logging.exception(e)
        traceback.print_exc()
        raise e


if __name__ == '__main__':
    main()
