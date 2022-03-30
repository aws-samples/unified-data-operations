# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
import sys, os
import traceback

from pyspark.sql import SparkSession
from driver import task_executor, packager
from .packager import ziplib
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


def init(spark_session: SparkSession = None, spark_config=None):
    global __SPARK__
    if not spark_session:
        __SPARK__ = get_or_create_session(spark_config)
    else:
        __SPARK__ = spark_session
    # sc  = __SPARK__.sparkContext
    # sc.setSystemProperty("com.amazonaws.services.s3.enableV4", "true")


def install_dependencies(product_path: str):
    new_packages = packager.install_dependencies(product_path)
    if new_packages:
        logger.info(f'Packaging up the following new dependencies {new_packages.keys()}')
        for new_pack_name in new_packages.keys():
            zipfile = ziplib(new_packages.get(new_pack_name), new_pack_name)
            logger.info(f'-----> installing {zipfile}')
            get_spark().sparkContext.addPyFile(zipfile)
        logger.debug('=> Dependencies are installed.')


def process_product(args, product_path: str):
    try:
        product = compile_product(product_path, args)
        models = compile_models(product_path, product)
        for task in product.pipeline.tasks:
            task_executor.execute(product, task, models, product_path)
    except Exception as e:
        traceback.print_exc()
        logger.error(f"Couldn't execute job due to >> {type(e).__name__}: {str(e)}")
        sys.exit(-1)
