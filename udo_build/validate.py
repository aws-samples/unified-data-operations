# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from udo_build.build_tools.configuration.core import DataProduct
from udo_build.build_tools.configuration.parser import parse
from udo_build.build_tools.configuration.error import ConfigurationError

logger = logging.getLogger(__name__)


def validate_configuration():
    try:
        data_product: DataProduct = parse()
        logger.info({
            'data_product_id': data_product.id,
            'data_product_version': data_product.version,
            'message': f'Successfully validated data product configuration',
        })
    except ConfigurationError as e:
        logger.exception({'message': 'Data product yaml configuration error'})
        raise e
    except Exception as e:
        logger.exception({'message': 'Unknown exception on validation step.'})
        raise e