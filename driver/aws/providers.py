# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

import boto3
import mypy_boto3_glue
from driver.core import (
    Connection,
    ConnectionNotFoundException,
    DataProductTable,
    TableNotFoundException,
)

__SESSION__ = None
logger = logging.getLogger(__name__)


def init(
    key_id: str = None,
    key_material: str = None,
    profile: str = None,
    region: str = None,
):
    global __SESSION__
    if key_id and key_material and region:
        __SESSION__ = boto3.Session(
            aws_access_key_id=key_id,
            aws_secret_access_key=key_material,
            region_name=region,
        )
    elif key_id and key_material and not region:
        __SESSION__ = boto3.Session(
            aws_access_key_id=key_id, aws_secret_access_key=key_material
        )
    elif profile and region:
        __SESSION__ = boto3.Session(profile_name=profile, region_name=region)
    elif profile and not region:
        __SESSION__ = boto3.Session(profile_name=profile)
    elif region:
        __SESSION__ = boto3.Session(region_name=region)
    else:
        __SESSION__ = boto3.Session()

    # amongst others used to verify bucket ownership in interaction with s3
    global __AWS_ACCOUNT_ID__
    sts = __SESSION__.client("sts")
    __AWS_ACCOUNT_ID__ = sts.get_caller_identity()["Account"]


def get_session() -> boto3.Session:
    return __SESSION__


def get_aws_account_id() -> str:
    if not __AWS_ACCOUNT_ID__:
        raise Exception("Boto session is not initialized. Please call init first.")
    return __AWS_ACCOUNT_ID__


def get_glue() -> mypy_boto3_glue.GlueClient:
    if not get_session():
        raise Exception("Boto session is not initialized. Please call init first.")
    return get_session().client("glue")


def get_s3():
    if not get_session():
        raise Exception("Boto session is not initialized. Please call init first.")

    return get_session().client("s3")


def connection_provider(connection_id: str) -> Connection:
    """
    Returns a data deprecated connection object, that can be used to connect to databases.
    :param connection_id:
    :return:
    """
    try:
        if not get_session():
            raise Exception("Boto session is not initialized. Please call init first.")
        glue = get_session().client("glue")
        response = glue.get_connection(Name=connection_id, HidePassword=False)
        if "Connection" not in response:
            raise ConnectionNotFoundException(
                f"Connection [{connection_id}] could not be found."
            )
        cprops = response.get("Connection").get("ConnectionProperties")
        native_host = cprops.get("JDBC_CONNECTION_URL")[len("jdbc:") :]
        connection = Connection.parse_obj(
            {
                "name": connection_id,
                "host": native_host,
                "principal": cprops.get("USERNAME"),
                "credential": cprops.get("PASSWORD"),
                "type": native_host.split(":")[0],
                "ssl": cprops.get("JDBC_ENFORCE_SSL"),
            }
        )
        return connection
    except Exception as e:
        raise ConnectionNotFoundException(
            f"Connection [{connection_id}] could not be found. {str(e)}"
        )


def datalake_provider(product_id, table_id) -> DataProductTable:
    if not get_session():
        raise Exception("Boto session is not initialized. Please call init first.")
    glue = get_session().client("glue")
    response = glue.get_table(DatabaseName=product_id, Name=table_id)
    if "Table" not in response:
        raise TableNotFoundException(
            f"Data Product Table [{product_id}.{table_id}] could not be found."
        )
    table = DataProductTable.parse_obj(
        {
            "product_id": product_id,
            "table_id": table_id,
            "storage_location": response.get("Table")
            .get("StorageDescriptor")
            .get("Location"),
        }
    )
    return table
