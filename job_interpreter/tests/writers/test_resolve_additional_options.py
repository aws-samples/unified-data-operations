from unittest.mock import Mock

from ...writers.data_frame import resolve_additional_options


def test_resolve_additional_options_no_partition_key():
    expected_additional_options = {'enableUpdateCatalog': True}

    actual_additional_options = resolve_additional_options({})

    assert actual_additional_options == expected_additional_options


def test_resolve_additional_options_with_partition_key():
    expected_additional_options = {'enableUpdateCatalog': True, 'partitionKeys': ['id']}

    actual_additional_options = resolve_additional_options({'partition_by': 'id'})

    assert actual_additional_options == expected_additional_options
