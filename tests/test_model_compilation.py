import os
from types import SimpleNamespace

import pytest
from jsonschema import ValidationError

from driver import util
from driver.core import ArtefactType
from driver.util import compile_product, compile_models, filter_list_by_id
from tests.conftest import DEFAULT_BUCKET


# @pytest.fixture
# def metadata_path():
#     cwd_path = os.path.dirname(os.path.abspath(__file__))
#     return os.path.join(cwd_path, 'assets', 'model_defs')


def test_basic_model_compilation(fixture_asset_path, app_args):
    product = compile_product(fixture_asset_path, app_args)
    models = compile_models(fixture_asset_path, product)
    assert product.engine == 'glue'
    assert product.id == 'some_data_product'
    assert product.owner == 'jane@acme.com'
    assert product.version == '1.0.0'
    assert product.defaults.storage.location == DEFAULT_BUCKET
    assert getattr(product, 'pipeline')
    assert getattr(product.pipeline, 'tasks')
    assert len(product.pipeline.tasks) == 1
    assert product.pipeline.tasks[0].id == 'process_some_files'
    assert len(product.pipeline.tasks[0].inputs) == 1
    assert len(product.pipeline.tasks[0].outputs) == 1
    inp = product.pipeline.tasks[0].inputs[0]
    assert hasattr(inp, 'connection')
    assert hasattr(inp, 'model')
    assert hasattr(inp, 'table')
    assert len(models) == 4


# def test_connection_with_model(metadata_path):
#     args = SimpleNamespace()
#     product = compile_product(metadata_path, args, prod_def_filename='product_correct_connection_w_model.yml')
#     models = compile_models(metadata_path, product, def_file_name='model_correct.yml')
#     assert len(models) == 2


# def test_minimal_model_compilation(product, models):
#     assert models


def test_advanced_compilation_features(fixture_asset_path, app_args):
    # abs_product_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'assets', 'advanced_compilation')
    product = compile_product(fixture_asset_path, app_args, prod_def_filename='product_compilation.yml')
    models = compile_models(fixture_asset_path, product, def_file_name='model_compilation.yml')
    assert len(models) == 2
    person_pii = filter_list_by_id(models, 'person_pii')
    assert person_pii.storage.location == DEFAULT_BUCKET, 'The default bucket should be set on models with no explicit location'
    person_pub = filter_list_by_id(models, 'person_pub')
    pub_full_name_col = filter_list_by_id(person_pub.columns, 'full_name')
    assert pub_full_name_col.type == 'string', 'The String type should have been inherited from the pii model'
    assert pub_full_name_col.transform[
               0].type == 'encrypt', 'The Transform should have beein inherited from the pii model'
    pub_full_id_col = filter_list_by_id(person_pub.columns, 'id')
    assert pub_full_id_col, 'ID col should have been inherited from the pii model'
    assert pub_full_id_col.type == 'integer', 'ID col type should have been inherited from the pii model'
    gender = filter_list_by_id(person_pub.columns, 'gender')
    assert gender, 'The model should inherit the Genre column from the person pii model'


# def test_mode_extend_compilation_non_specified_field():
#     abs_product_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'assets')
#     args = SimpleNamespace()
#     product = compile_product(abs_product_path, args)
#     models = compile_models(abs_product_path, product)


def test_model_schema_correct(fixture_asset_path):
    product_def = util.load_yaml(os.path.join(fixture_asset_path, 'model_correct.yml'))
    util.validate_schema(product_def, ArtefactType.models)


def test_product_schema_correct(fixture_asset_path):
    product_def = util.load_yaml(os.path.join(fixture_asset_path, 'product_correct.yml'))
    util.validate_schema(product_def, ArtefactType.product)


def test_product_schema_correct_with_models(fixture_asset_path):
    product_def = util.load_yaml(os.path.join(fixture_asset_path, 'product_correct_all_models.yml'))
    util.validate_schema(product_def, ArtefactType.product)


def test_product_schema_wrong_engine(fixture_asset_path):
    product_def = util.load_yaml(os.path.join(fixture_asset_path, 'product_wrong_engine.yml'))
    with pytest.raises(ValidationError) as vex:
        util.validate_schema(product_def, ArtefactType.product)


def test_product_schema_output_err(fixture_asset_path):
    # missing module parameters
    product_def = util.load_yaml(os.path.join(fixture_asset_path, 'product_wrong_output.yml'))
    with pytest.raises(ValidationError) as vex:
        util.validate_schema(product_def, ArtefactType.product)


def test_product_missing_logic(fixture_asset_path):
    product_def = util.load_yaml(os.path.join(fixture_asset_path, 'product_missing_logic.yml'))
    util.validate_schema(product_def, ArtefactType.product)

    product_def = util.load_yaml(os.path.join(fixture_asset_path, 'product_correct_missing_logic_params.yml'))
    util.validate_schema(product_def, ArtefactType.product)
