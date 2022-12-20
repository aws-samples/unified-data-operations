# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import json
from urllib.parse import urlparse

from jsonschema import validate, ValidationError
import os
from types import SimpleNamespace
from dataclasses import dataclass
from pyspark.sql import DataFrame
from enum import Enum
from pydantic import (
    BaseModel,
    AnyUrl,
    SecretStr,
    conint,
    validator, root_validator, parse_obj_as, ValidationError, error_wrappers, Field)
from typing import Dict, List, Tuple, Any, TypeVar, Union
from pydantic import AnyUrl
from driver import util

Scalar = TypeVar('Scalar', int, float, bool, str)


@dataclass
class DataProduct:
    id: str
    description: str = None
    owner: str = None


@dataclass
class DataSet:
    id: str
    df: DataFrame
    model: SimpleNamespace = None
    product: DataProduct = None

    @classmethod
    def find_by_id(cls, dataset_list, ds_id):
        return next(iter([m for m in dataset_list if m.id == ds_id]), None)

    @property
    def partitions(self) -> List[str]:
        if self.storage_options and hasattr(self.storage_options, 'partition_by'):
            if isinstance(self.storage_options.partition_by, str):
                return [self.storage_options.partition_by]
            else:
                return [p for p in self.storage_options.partition_by]
        else:
            return list()

    @property
    def storage_location(self) -> str:
        if util.check_property(self, 'model.storage.location'):
            return self.model.storage.location
        else:
            return None

    @storage_location.setter
    def storage_location(self, path: str):
        if not self.model:
            raise Exception("There's no model on the dataset, so location cannot be set yet.")
        elif not hasattr(self.model, 'storage'):
            storage = SimpleNamespace()
            setattr(storage, 'location', path)
            setattr(self.model, 'storage', storage)
        elif not hasattr(self.model.storage, 'location'):
            setattr(self.model.storage, 'location', path)
        else:
            self.model.storage.location = path

    @property
    def path(self) -> str:
        if self.id is None:
            raise Exception(f'Can not construct data set path because product id is not defined.')
        if not self.storage_location:
            raise Exception(f'The data set storage location is not set for dataset id: {self.id}.')
        return f"{self.product.id}/{self.id}"

    @property
    def dataset_storage_path(self) -> str:
        return f'{self.storage_location}/{self.path}'

    @property
    def storage_type(self) -> str:
        if self.model and hasattr(self.model, 'storage'):
            return self.model.storage.type
        else:
            return 'default'

    @property
    def storage_format(self) -> str:
        if self.model and hasattr(self.model, 'storage'):
            return self.model.storage.format if hasattr(self.model.storage, 'format') else None
        else:
            return None

    @property
    def storage_options(self) -> SimpleNamespace:
        if self.model and hasattr(self.model, 'storage') and hasattr(self.model.storage, 'options'):
            return self.model.storage.options
        else:
            return None

    @property
    def product_id(self) -> str:
        return self.product.id if self.product else None

    @product_id.setter
    def product_id(self, p_id: str) -> None:
        if self.product:
            self.product.id = p_id
        else:
            self.product = DataProduct(id=p_id)

    @property
    def product_description(self) -> str:
        return self.product.description if self.product else None

    @property
    def product_owner(self) -> str:
        return self.product.owner if self.product else None

    @property
    def tags(self) -> dict:
        if not hasattr(self, 'model') or not hasattr(self.model, 'tags'):
            return dict()
        if self.id is None:
            raise Exception(f'Can not construct tags, id is not defined.')
        return self.model.tags.__dict__

    @property
    def access_tags(self) -> dict:
        if not hasattr(self, 'model') or not hasattr(self.model, 'access'):
            return dict()
        if self.id is None:
            raise Exception(f'Can not construct tags, id is not defined.')
        return self.model.access.__dict__

    @property
    def all_tags(self) -> dict:
        if self.id is None:
            raise Exception(f'Can not construct tags, id is not defined.')
        return {**self.tags, **{'access_' + k: v for k, v in self.access_tags.items()}}

    @property
    def model_name(self) -> str:
        return self.model.name if hasattr(self, 'model') and hasattr(self.model, 'name') else self.id

    @property
    def model_description(self) -> str:
        return self.model.description if hasattr(self, 'model') and hasattr(self.model, 'description') else str()


class SchemaValidationException(Exception):
    def __init__(self, message: str, data_set: DataSet):
        self.data_set = data_set
        super().__init__(message)


class ValidationException(Exception):
    def __init__(self, message: str):
        super().__init__(message)


class ConnectionNotFoundException(Exception):
    pass


class TableNotFoundException(Exception):
    pass


class JobExecutionException(Exception):
    pass


class ProcessorChainExecutionException(Exception):
    pass


class ResolverException(Exception):
    pass


class LocationDsn(AnyUrl):
    allowed_schemes = {'datastore', 'connection'}
    user_required = False


class PostgresDsn(AnyUrl):
    allowed_schemes = {'postgres', 'postgresql'}
    user_required = False


class JdbcDsn(AnyUrl):
    allowed_schemes = {'jdbc', 'jdbc'}
    user_required = False


class MysqlDsn(AnyUrl):
    allowed_schemes = {'mysql', 'mysql'}
    user_required = False


class IOType(str, Enum):
    model = 'model'
    connection = 'connection'
    file = 'file'


class ArtefactType(str, Enum):
    models = 'model'
    product = 'product'


class ConnectionType(str, Enum):
    jdbc = 'jdbc'
    postgresql = 'postgresql'
    redshift = 'redshift'
    mysql = 'mysql'
    mariadb = 'mariadb'
    mongodb = 'mongodb'
    s3 = 's3'
    csv = 'csv'
    parquet = 'parquet'

    @classmethod
    def is_file(cls, conn_type: 'ConnectionType'):
        return conn_type in [ConnectionType.csv, ConnectionType.parquet, ConnectionType.s3]


url_parsers = {
    ConnectionType.postgresql: PostgresDsn,
    ConnectionType.jdbc: JdbcDsn
}


class Connection(BaseModel):
    name: str
    principal: Union[str, None]
    credential: Union[SecretStr, None]
    host: str
    port: Union[conint(lt=65535), None]
    db_name: Union[str, None]
    ssl: bool = False
    type: ConnectionType
    timeout: int = 3600
    batch_size: int = 10000
    meta_data: Dict[str, Scalar] = {}

    class Config:
        validate_assignment = True

    @classmethod
    def is_port_required(cls, conn_type: Union[ConnectionType, str]):
        if isinstance(conn_type, str):
            conn_type = ConnectionType(conn_type)
        return not ConnectionType.is_file(conn_type)

    @classmethod
    def is_jdbc_supported(cls, conn_type: Union[ConnectionType, str]):
        return Connection.is_port_required(conn_type)

    @classmethod
    def is_db_name_required(cls, conn_type: Union[ConnectionType, str]):
        return Connection.is_port_required(conn_type)

    @classmethod
    def is_userinfo_required(cls, conn_type: Union[ConnectionType, str]):
        return Connection.is_port_required(conn_type)

    @classmethod
    def fill_url_contained_values(cls, values: dict, ctype: Union[ConnectionType, str]):
        def strip_path(string: str):
            return string.strip('/')

        validable_keys = ['principal', 'credential', 'port', 'db_name']
        autofill_checkers = {
            'port': Connection.is_port_required,
            'principal': Connection.is_userinfo_required,
            'credential': Connection.is_userinfo_required,
            'db_name': Connection.is_db_name_required,
        }
        url_property_map = {
            'port': ('port', None),
            'host': ('host', None),
            'principal': ('user', None),
            'credential': ('password', None),
            'db_name': ('path', strip_path)
        }
        none_valued_keys = [k for k in values.keys() if not values.get(k)]
        values_keys = set(list(values.keys()) + none_valued_keys)
        vk = set(validable_keys)
        missing_keys = vk.difference(values_keys)
        parsable_keys = []
        for k in missing_keys:
            if autofill_checkers.get(k)(ctype):
                parsable_keys.append(k)
            else:
                values[k] = None
        if len(parsable_keys) == 0:
            return
        url_parser = url_parsers.get(ctype, AnyUrl)
        try:
            url: AnyUrl = parse_obj_as(url_parser, values.get('host'))
            for pk in parsable_keys:
                func_name, converter = url_property_map.get(pk)
                value = getattr(url, func_name)
                if not value:
                    raise ValueError(f'The field {pk} is required and not provided in the url or directly.')
                if converter:
                    values[pk] = converter(value)
                else:
                    values[pk] = value
        except ValueError as verr:
            raise verr
        except TypeError as tep:
            raise ValueError(
                f'Programming error at Connection Validation: {str(tep)}. '
                f'Function name for property to be invoked on URL of type {type(url)}: {func_name}')
        except Exception as ex:
            raise ValueError(
                f'When one of the following fields is missing {validable_keys}, '
                f'the $host URL must include its value; {str(ex)}')

    def get_native_connection_url(self, generate_creds=True) -> str:
        url_parser = url_parsers.get(self.type, AnyUrl)
        try:
            url: AnyUrl = parse_obj_as(url_parser, self.host)
            if Connection.is_userinfo_required(self.type):
                user = url.user or self.principal
                password = url.password or self.credential.get_secret_value()
            if Connection.is_db_name_required(self.type):
                path = url.path or f'/{self.db_name}'
            if Connection.is_port_required(self.type):
                port = url.port or self.port
            if generate_creds:
                return AnyUrl.build(scheme=url.scheme, user=user, password=password, host=url.host, port=port,
                                    path=path)
            else:
                return AnyUrl.build(scheme=url.scheme, host=url.host, port=port, path=path)
        except (error_wrappers.ValidationError, ValidationError):
            # not a url format
            passwd = self.credential.get_secret_value() if self.credential else ''
            userinfo = f'{self.principal}:{passwd}@' if Connection.is_userinfo_required(self.type) else ''
            host = self.host.strip('/') if self.host else ''
            port = f':{self.port}' if Connection.is_port_required(self.type) else ''
            db_path = f'/{self.db_name}' if Connection.is_db_name_required(self.type) else ''
            return f'{str(self.type.value)}://{userinfo}{host}{port}{db_path}'

    def get_jdbc_connection_url(self, generate_creds=True) -> str:
        if Connection.is_jdbc_supported(self.type):
            return f'jdbc:{self.get_native_connection_url(generate_creds)}'
        else:
            raise AssertionError(f"The connection {self.type.value} doesn't support JDBC.")

    @root_validator(pre=True)
    def check_host_url_dependent_fields(cls, values: dict):
        connection_type = values.get('type')
        host = values.get('host')
        if not host or not connection_type:
            raise ValueError('The host and the connection type must be defined.')
        Connection.fill_url_contained_values(values, connection_type)
        return values


class DataProductTable(BaseModel):
    product_id: str
    table_id: str
    storage_location: str

    @property
    def storage_location_s3a(self):
        return self.storage_location.replace('s3://', 's3a://')


def resolve_data_set_id(io_def: SimpleNamespace) -> str:
    def xtract_domain(s):
        if '.' in s:
            domain_elements = s.rsplit('.')
            return domain_elements[len(domain_elements) - 1]
        else:
            return s

    if io_def.type == IOType.model:
        model_url = getattr(io_def, io_def.type)
        return xtract_domain(model_url)
    elif io_def.type == IOType.connection:
        return xtract_domain(io_def.model) if hasattr(io_def, 'model') else xtract_domain(io_def.table)
    elif io_def.type == IOType.file:
        if hasattr(io_def, IOType.model.name):
            return xtract_domain(getattr(io_def, IOType.model.name))
        else:
            parsed_file = urlparse(io_def.file)
            filename = os.path.basename(parsed_file.path)
            return filename.rsplit('.')[0]
    else:
        raise ConnectionNotFoundException(f'The IO Type {io_def.type} is not supported.')


def resolve_data_product_id(io_def: SimpleNamespace) -> str:
    if io_def.type == IOType.model:
        return getattr(io_def, io_def.type).rsplit('.')[0]
    elif io_def.type == IOType.connection:
        return getattr(io_def, 'table').rsplit('.')[0]
