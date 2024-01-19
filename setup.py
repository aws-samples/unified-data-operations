# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from os import path, system
from pip._internal.req import parse_requirements
from setuptools import setup, find_packages, Command

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

requirements = [str(ir.requirement) for ir in parse_requirements(
    'requirements.txt', session=False)]


class CleanCommand(Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        system(
            'rm -vrf ./build ./dist ./*.pyc ./*.tgz ./*.egg-info ./htmlcov '
            './spark-warehouse ./driver/spark-warehouse ./metastore_db ./coverage_html ./.pytest_cache ./derby.log ./tests/local_results ./tasks/__pycache__')


setup(
    name="unified-data-platform",
    version="1.0.5",
    description="Data Operations Automation for executing vanilla Spark and SQL.",
    long_description=long_description,
    long_description_content_type='text/markdown',
    author="Amazon Web Services",
    url='https://github.com/aws-samples/unified-data-operations',
    packages=find_packages(
        exclude=(
            "contrib",
            "docs",
            "tests",
        )
    ),
    py_modules=[
        'main',
    ],
    install_requires=requirements,
    include_package_data=True,
    platforms="any",
    license="Apache License 2.0",
    zip_safe=False,
    cmdclass={
        'clean_all': CleanCommand,
        # 'package': Package
    },
    entry_points={
        "console_scripts": [
            "data-product-processor=main:main",
            "udo-build=udo_build.main:main",
            "udo=cli.main:main"],
    },
)
