# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from .core import DataProduct
from .dpp_adapter import parse as parse_dpp


def parse(source_path: str = ".") -> DataProduct:
    return parse_dpp(source_path)
