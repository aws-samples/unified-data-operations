import os
from types import SimpleNamespace

import pytest
from jsonschema import ValidationError

from driver import util
from driver.core import ArtefactType
from driver.util import compile_product, compile_models


@pytest.fixture
def metadata_path():
    cwd_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cwd_path, 'assets', 'schema_defs')


def test_schema_compilation():
    args = SimpleNamespace()
    abs_product_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'assets')
    product = compile_product(abs_product_path, args)
    models = compile_models(abs_product_path, product)


def test_model_schema_correct(metadata_path):
    product_def = util.load_yaml(os.path.join(metadata_path, 'model_correct.yml'))
    util.validate_schema(product_def, ArtefactType.models)


def test_product_schema_correct(metadata_path):
    product_def = util.load_yaml(os.path.join(metadata_path, 'product_correct.yml'))
    util.validate_schema(product_def, ArtefactType.product)


def test_product_schema_correct_with_models(metadata_path):
    product_def = util.load_yaml(os.path.join(metadata_path, 'product_correct_all_models.yml'))
    util.validate_schema(product_def, ArtefactType.product)


def test_product_schema_wrong_engine(metadata_path):
    product_def = util.load_yaml(os.path.join(metadata_path, 'product_wrong_engine.yml'))
    with pytest.raises(ValidationError) as vex:
        util.validate_schema(product_def, ArtefactType.product)


def test_product_schema_output_err(metadata_path):
    # missing module parameters
    product_def = util.load_yaml(os.path.join(metadata_path, 'product_wrong_output.yml'))
    with pytest.raises(ValidationError) as vex:
        util.validate_schema(product_def, ArtefactType.product)

    # product_def.get('product').get('pipeline').get('tasks')[0].get('outputs')


def test_product_missing_logic(metadata_path):
    product_def = util.load_yaml(os.path.join(metadata_path, 'product_missing_logic.yml'))
    util.validate_schema(product_def, ArtefactType.product)

    product_def = util.load_yaml(os.path.join(metadata_path, 'product_correct_missing_logic_params.yml'))
    util.validate_schema(product_def, ArtefactType.product)
