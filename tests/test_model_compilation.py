import os
from types import SimpleNamespace

import pytest
from jsonschema import ValidationError

from driver import util
from driver.core import ArtefactType
from driver.util import compile_product, compile_models, filter_list_by_id


@pytest.fixture
def metadata_path():
    cwd_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(cwd_path, 'assets', 'schema_defs')


def test_basic_model_compilation():
    default_bucket = 's3://test-bucket'
    args = SimpleNamespace()
    setattr(args, 'default_data_lake_bucket', default_bucket)
    abs_product_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'assets')
    product = compile_product(abs_product_path, args)
    models = compile_models(abs_product_path, product)
    assert product.engine == 'glue'
    assert product.id == 'customers'
    assert product.owner == 'jane@acme.com'
    assert product.version == '1.0.0'
    assert product.defaults.storage.location == default_bucket
    assert getattr(product, 'pipeline')
    assert getattr(product.pipeline, 'tasks')
    assert len(product.pipeline.tasks) == 1
    assert product.pipeline.tasks[0].id == 'extract_customers'
    assert len(product.pipeline.tasks[0].inputs) == 1
    assert len(product.pipeline.tasks[0].outputs) == 1
    inp = product.pipeline.tasks[0].inputs[0]
    assert hasattr(inp, 'connection')
    assert hasattr(inp, 'model')
    assert hasattr(inp, 'table')
    assert len(models) == 1


def test_minimal_model_compilation(product, models):
    assert models


def test_model_default_storage_compilation():
    default_bucket = 's3://test-bucket'
    args = SimpleNamespace()
    setattr(args, 'default_data_lake_bucket', default_bucket)
    abs_product_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'assets', 'advanced_compilation')
    product = compile_product(abs_product_path, args)
    models = compile_models(abs_product_path, product)
    assert len(models) == 2
    person_pii = filter_list_by_id(models, 'person_pii')
    assert person_pii.storage.location == default_bucket


def test_model_extend_compilation():
    args = SimpleNamespace()
    setattr(args, 'default_data_lake_bucket', 's3://test-bucket')
    abs_product_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'assets', 'advanced_compilation')
    product = compile_product(abs_product_path, args)
    models = compile_models(abs_product_path, product)
    assert len(models) == 2
    person_pub = filter_list_by_id(models, 'person_pub')
    pub_full_name_col = filter_list_by_id(person_pub.columns, 'full_name')
    assert pub_full_name_col.type == 'string'
    assert pub_full_name_col.transform[0].type == 'encrypt'
    pub_full_id_col = filter_list_by_id(person_pub.columns, 'id')
    assert pub_full_id_col
    assert pub_full_id_col.type == 'integer'
    genre = filter_list_by_id(person_pub.columns, 'genre')
    assert genre, 'The model should inherit the Genre column from the person pii model'


# def test_mode_extend_compilation_non_specified_field():
#     abs_product_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'assets')
#     args = SimpleNamespace()
#     product = compile_product(abs_product_path, args)
#     models = compile_models(abs_product_path, product)


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
