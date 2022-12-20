# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path
from setuptools import setup, find_packages

from pip._internal.req import parse_requirements
requirements = [
    str(ir.requirement) for ir in parse_requirements("requirements.txt", session=False)
]

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="data-product-processor",
    version="0.0.2",
    description="The data product processor (dpp) is a library for dynamically creating and executing Apache Spark Jobs based on a declarative description of a data product. A data product describes 1-to-many data sets that are to be created.",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author="Amazon Web Services",
    url = 'https://github.com/aws-samples/dpac-data-product-processor',
    packages=find_packages(
        exclude=(
            "contrib",
            "docs",
            "tests",
        )
    ),
    install_requires=requirements,
    include_package_data=True,
    platforms="any",
    license="Apache License 2.0",
    zip_safe=False,
    entry_points={
        "console_scripts": ["data-product-processor=main:main"],
    },
)
