# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0


from abc import ABC
from enum import Enum
from typing import List
import typing


class Input(ABC):
    pass


class FileInput(Input):
    def __init__(self, uri: str, model: str = None):
        self.uri = uri
        self.model = model


class ConnectionInput(Input):
    def __init__(self, connection: str, schema: str, table: str):
        self.connection = connection
        self.schema = schema
        self.table = table


class ModelInput(Input):
    def __init__(self, parent_data_product_id: str, model_id: str):
        self.parent_data_product_id = parent_data_product_id
        self.model_id = model_id


class Output(ABC):
    pass


class ModelOutput(Input):
    def __init__(self, model_id: str):
        self.model_id = model_id


class Task():
    def __init__(self, id: str):
        self.id = id
        self.inputs: List[Input] = []
        self.outputs: List[Output] = []

    def add_input(self, input: Input):
        self.inputs.append(input)

    def add_output(self, output: Output):
        self.outputs.append(output)


class Pipeline:
    def __init__(self, schedule: str):
        self.schedule = schedule
        self.tasks: List[Task] = []

    def add_task(self, task: Task):
        self.tasks.append(task)


class ColumnType(Enum):
    string = "string"
    double = "double"
    integer = "integer"
    undefined = "undefined"


class Column:
    def __init__(self, type: str, id: str, name: str, description: str, constraints: List[str], transforms: List[str]):
        self.type = type
        self.id = id
        self.name = name
        self.description = description
        self.constraints = constraints
        self.transforms = transforms


class ModelMetadata:
    def __init__(self, contains_pii: bool):
        self.contains_pii = contains_pii


class ModelAccess:
    def __init__(self, domain: str, confidentiality: str):
        self.domain = domain
        self.confidentiality = confidentiality


class Model:
    def __init__(self, id: str, name: str, version: str, description: str, metadata: ModelMetadata, access: ModelAccess, tags: typing.Dict[str, str]):
        self.id = id
        self.name = name
        self.version = version
        self.description = description
        self.columns: List[Column] = []
        self.metadata = metadata
        self.access = access
        self.tags = tags

    def add_tag(self, key: str, value: str):
        self.tags.update({key: value})

    def add_column(self, column: Column):
        self.columns.append(column)


class DataProduct:
    def __init__(self, id: str, name: str, version: str, description: str, owner: str, pipeline: Pipeline):
        self.id = id
        self.name = name
        self.version = version
        self.description = description
        self.owner = owner
        self.pipeline = pipeline
        self.models: List[Model] = []

    def add_model(self, model: Model):
        self.models.append(model)
