from os import path, system
from setuptools import setup, Command, find_packages
# from package import Package
from pip._internal.req import parse_requirements

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
    name='data-product-processor',
    version='1.0.0',
    description='Interprets Glue job definitions in the YAML format.',
    long_description=long_description,
    author='Michael Lewkowski',
    author_email='mlewk@amazon.de',
    packages=find_packages(exclude=('contrib', 'docs', 'tests',)),
    install_requires=requirements,
    include_package_data=True,
    platforms='any',
    license='Apache License 2.0',
    zip_safe=False,
    cmdclass={
        'clean_all': CleanCommand,
        # 'package': Package
    })
