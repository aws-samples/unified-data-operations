# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os

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
    if not (product.engine == "glue"):
        raise AssertionError
    if not (product.id == "some_data_product"):
        raise AssertionError
    if not (product.owner == "jane@acme.com"):
        raise AssertionError
    if not (product.version == "1.0.0"):
        raise AssertionError
    if not (product.defaults.storage.location == DEFAULT_BUCKET):
        raise AssertionError
    if not (getattr(product, "pipeline")):
        raise AssertionError
    if not (getattr(product.pipeline, "tasks")):
        raise AssertionError
    if not (len(product.pipeline.tasks) == 1):
        raise AssertionError
    if not (product.pipeline.tasks[0].id == "process_some_files"):
        raise AssertionError
    if not (len(product.pipeline.tasks[0].inputs) == 1):
        raise AssertionError
    if not (len(product.pipeline.tasks[0].outputs) == 1):
        raise AssertionError
    inp = product.pipeline.tasks[0].inputs[0]
    if not (hasattr(inp, "connection")):
        raise AssertionError
    if not (hasattr(inp, "model")):
        raise AssertionError
    if not (hasattr(inp, "table")):
        raise AssertionError
    if not (len(models) == 4):
        raise AssertionError


# def test_connection_with_model(metadata_path):
#     args = SimpleNamespace()
#     product = compile_product(metadata_path, args, prod_def_filename='product_correct_connection_w_model.yml')
#     models = compile_models(metadata_path, product, def_file_name='model_correct.yml')
#     assert len(models) == 2


# def test_minimal_model_compilation(product, models):
#     assert models


def test_advanced_compilation_features(fixture_asset_path, app_args):
    # abs_product_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'assets', 'advanced_compilation')
    product = compile_product(
        fixture_asset_path, app_args, prod_def_filename="product_compilation.yml"
    )
    models = compile_models(
        fixture_asset_path, product, def_file_name="model_compilation.yml"
    )
    if not (len(models) == 2):
        raise AssertionError
    person_pii = filter_list_by_id(models, "person_pii")
    if not (
        person_pii.storage.location == DEFAULT_BUCKET,
        "The default bucket should be set on models with no explicit location",
    ):
        raise AssertionError
    if not (person_pii.storage.type == "lake"):
        raise AssertionError
    person_pub = filter_list_by_id(models, "person_pub")
    if not (person_pub.storage.type == "lake"):
        raise AssertionError
    pub_full_name_col = filter_list_by_id(person_pub.columns, "full_name")
    if not (
        pub_full_name_col.type == "string",
        "The String type should have been inherited from the pii model",
    ):
        raise AssertionError
    if not (
        pub_full_name_col.transform[0].type == "encrypt",
        "The Transform should have beein inherited from the pii model",
    ):
        raise AssertionError
    pub_full_id_col = filter_list_by_id(person_pub.columns, "id")
    if not (pub_full_id_col, "ID col should have been inherited from the pii model"):
        raise AssertionError
    if not (
        pub_full_id_col.type == "integer",
        "ID col type should have been inherited from the pii model",
    ):
        raise AssertionError
    gender = filter_list_by_id(person_pub.columns, "gender")
    if not (
        gender,
        "The model should inherit the Genre column from the person pii model",
    ):
        raise AssertionError


# def test_mode_extend_compilation_non_specified_field():
#     abs_product_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'assets')
#     args = SimpleNamespace()
#     product = compile_product(abs_product_path, args)
#     models = compile_models(abs_product_path, product)


def test_model_schema_correct(fixture_asset_path):
    product_def = util.load_yaml(os.path.join(fixture_asset_path, "model_correct.yml"))
    util.validate_schema(product_def, ArtefactType.models)


def test_product_schema_correct(fixture_asset_path):
    product_def = util.load_yaml(
        os.path.join(fixture_asset_path, "product_correct.yml")
    )
    util.validate_schema(product_def, ArtefactType.product)


def test_product_schema_correct_with_models(fixture_asset_path):
    product_def = util.load_yaml(
        os.path.join(fixture_asset_path, "product_correct_all_models.yml")
    )
    util.validate_schema(product_def, ArtefactType.product)


def test_product_schema_wrong_engine(fixture_asset_path):
    product_def = util.load_yaml(
        os.path.join(fixture_asset_path, "product_wrong_engine.yml")
    )
    with pytest.raises(ValidationError) as vex:
        util.validate_schema(product_def, ArtefactType.product)


def test_product_schema_output_err(fixture_asset_path):
    # missing module parameters
    product_def = util.load_yaml(
        os.path.join(fixture_asset_path, "product_wrong_output.yml")
    )
    with pytest.raises(ValidationError) as vex:
        util.validate_schema(product_def, ArtefactType.product)


def test_product_missing_logic(fixture_asset_path):
    product_def = util.load_yaml(
        os.path.join(fixture_asset_path, "product_missing_logic.yml")
    )
    util.validate_schema(product_def, ArtefactType.product)

    product_def = util.load_yaml(
        os.path.join(fixture_asset_path, "product_correct_missing_logic_params.yml")
    )
    util.validate_schema(product_def, ArtefactType.product)


def test_connection_input_configuration(fixture_asset_path):
    pass


def test_model_input_configuration(fixture_asset_path):
    pass


def test_file_input_configuration(fixture_asset_path):
    # product_input_file.yml
    pass
