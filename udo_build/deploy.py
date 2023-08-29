# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


import os
import typing

import jinja2
import logging
import shutil
from os import getenv
from datetime import datetime
from typing import List
import boto3
from udo_build.build_tools.configuration.core import DataProduct, ModelInput
from udo_build.build_tools.configuration.parser import parse

logger = logging.getLogger(__name__)
logger.setLevel('INFO')
s3_client = boto3.resource('s3')
sts = boto3.client('sts')
code_pipeline = boto3.client('codepipeline')

# templating of dags
TEMPLATE_BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
DAGS_S3_BUCKET = getenv('DAG_S3_BUCKET')
DAGS_S3_KEY_PREFIX = getenv('DAGS_S3_KEY_PREFIX') or 'dags'

# sources of data products on the metadata bucket
S3_DATA_PRODUCT_DEFINITIONS = getenv('S3_DATA_PRODUCT_DEFINITIONS')
S3_DATALAKE = getenv('S3_DATALAKE')

# data-product-processor-based computation on Glue
GLUE_ROLE_NAME = getenv('DAG_GLUE_ROLE_NAME')
DPP_VERSION = getenv('DPP_VERSION')
PYSPARK_SCRIPT_LOCATION = f's3://{getenv("S3_ETL_ARTIFACTS")}/main.py'
S3_BUCKET_TEMP = f's3://{getenv("S3_BUCKET_TEMP")}/glue'

# make all files found under extra_jars available to Glue via --extra-jars argument
etl_artifact_bucket = s3_client.Bucket(getenv('S3_ETL_ARTIFACTS'))
jars = []
for jar in etl_artifact_bucket.objects.filter(Prefix='extra_jars/'):
    if jar.key != 'extra_jars/':
        jars.append(f's3://{getenv("S3_ETL_ARTIFACTS")}/{jar.key}')
EXTRA_JARS = ','.join(jars)

# Airflow notifications
NOTIFICATION_SENDER_EMAIL_ADDRESS = getenv('NOTIFICATION_SENDER_EMAIL_ADDRESS')
AIRFLOW_BASE_URL = getenv('AIRFLOW_BASE_URL')


def deploy_data_product_dag() -> DataProduct:
    try:
        data_product: DataProduct = parse()
        # the jinja2 context is independent of the execution engine (dpp etc.)
        template_parameters = {
            'dag_id': data_product.id,
            'product': data_product,
            'dependencies': get_dependencies(data_product),
            'now': datetime.utcnow(),
            'NOTIFICATION_SENDER_EMAIL_ADDRESS': NOTIFICATION_SENDER_EMAIL_ADDRESS,
            'GLUE_VERSION': '3.0',
            'GLUE_PYTHON_VERSION': '3',
            'S3_BUCKET_RESULTS': S3_DATALAKE,
            'S3_BUCKET_ARTIFACTS': S3_DATA_PRODUCT_DEFINITIONS,
            'S3_BUCKET_TEMP': S3_BUCKET_TEMP,
            'GLUE_ROLE_NAME': GLUE_ROLE_NAME,
            'DPP_VERSION': DPP_VERSION,
            'PYSPARK_SCRIPT_LOCATION': PYSPARK_SCRIPT_LOCATION,
            'EXTRA_JARS': EXTRA_JARS,
            'AIRFLOW_BASE_URL': AIRFLOW_BASE_URL,
            'AWS_REGION': os.environ['AWS_REGION']
        }

        template_loader = jinja2.FileSystemLoader(searchpath=TEMPLATE_BASE_DIR)
        template_env = jinja2.Environment(loader=template_loader)

        template = template_env.get_template('dpp.py.jinja')
        rendered_dag_content = template.render(template_parameters)

        dag_s3_key = f'{DAGS_S3_KEY_PREFIX}/{data_product.id}.py'
        dag_file = s3_client.Object(DAGS_S3_BUCKET, dag_s3_key)
        logger.info(f'Putting DAG file {dag_file.key} to {dag_file.bucket_name}.')
        dag_file.put(Body=rendered_dag_content.encode(
        ), ExpectedBucketOwner=sts.get_caller_identity().get('Account'))
        logger.info('DAG has been created and deployed successfully')
        return data_product
    except Exception as e:
        logger.exception('DAG creation has failed')
        raise e


def package_data_product_version(data_product: DataProduct, data_product_path: str = '.') -> typing.Tuple[str, str]:
    logger.info('Packaging new data product version')
    version_name = f'{data_product.id}-{data_product.version}'
    version_location = f'dist/{version_name}'
    logger.info(f'Starting make archive process at {os.path.join(os.path.dirname(os.path.abspath(__file__)))}')
    shutil.make_archive(version_location, format='zip', root_dir=data_product_path)
    logger.info(f'Data product version {version_name} successfully created')
    return f'{version_name}.zip', f'{version_location}.zip'


def upload_data_product_version(version_name: str, version_location: str):
    logger.info(f'Starting version upload into {S3_DATA_PRODUCT_DEFINITIONS} bucket')
    data_product_s3_key = f'datamesh/products/{version_name}'
    data_product_version_object = s3_client.Object(S3_DATA_PRODUCT_DEFINITIONS, data_product_s3_key)
    data_product_version_object.upload_file(version_location)
    logger.info(f'New data product version available in s3://{S3_DATA_PRODUCT_DEFINITIONS}/{data_product_s3_key}')


def get_dependencies(product: DataProduct) -> List[str]:
    # a data product can depend on multiple output models of a single upstream (parent) data product
    # in this case, per upstream data product, we want to have a single dependency in the dag
    # the 'set' data type guarantees not to have duplicate items
    dependencies = []
    for task in product.pipeline.tasks:
        for task_input in task.inputs:
            if isinstance(task_input, ModelInput):
                product_id = task_input.parent_data_product_id
                # todo: assuming version 1.0.0, as version is currently missing in input.model in product.yml
                version = '1.0.0'
                dependencies.append(f'{product_id}-{version}')
    return dependencies
