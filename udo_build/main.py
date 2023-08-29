# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import argparse
import logging

from udo_build.build_tools.configuration.core import DataProduct
from udo_build.deploy import deploy_data_product_dag, package_data_product_version, upload_data_product_version
from udo_build.validate import validate_configuration

logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel('INFO')


def execute(args):
    if args.validate_config:
        logger.info('Validate data product configuration of product and model YAML files')
        validate_configuration()
        logger.info('Validation of data product completed')

    if args.deploy_data_product:
        logger.info('Deploy data product into airflow')
        data_product: DataProduct = deploy_data_product_dag()
        version_name, version_location = package_data_product_version(data_product)
        upload_data_product_version(version_name, version_location)
        logger.info('Deployment of data product completed')


def main():
    try:
        parser = argparse.ArgumentParser(argument_default=argparse.SUPPRESS)
        parser.add_argument(
            '--validate-config',
            action='store_true',
            default=False,
            help='validate data product configuration'
        )
        parser.add_argument(
            '--deploy-data-product',
            action='store_true',
            default=False,
            help='deploy data product to orchestrator'
        )
        args, unknown = parser.parse_known_args()
        logger.info(args)
        execute(args)
    except Exception as e:
        logging.exception(e)
        raise e


if __name__ == "__main__":
    main()
