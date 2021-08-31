import boto3
from .core import Connection, ConnectionNotFoundException

__SESSION__ = None


def init(key_id: str = None, key_material: str = None, profile: str = None, region: str = None):
    global __SESSION__
    if key_id and key_material and region:
        __SESSION__ = boto3.Session(aws_access_key_id=key_id, aws_secret_access_key=key_material, region_name=region)
    elif key_id and key_material and not region:
        __SESSION__ = boto3.Session(aws_access_key_id=key_id, aws_secret_access_key=key_material)
    elif profile and region:
        __SESSION__ = boto3.Session(profile_name=profile, region_name=region)
    elif profile and not region:
        __SESSION__ = boto3.Session(profile_name=profile)
    elif region:
        __SESSION__ = boto3.Session(region_name=region)
    else:
        __SESSION__ = boto3.Session()


def get_session():
    return __SESSION__


def connection_provider(connection_id: str) -> Connection:
    if not __SESSION__:
        raise Exception('Boto session is not initialized. Please call init first.')
    glue = get_session().client('glue')
    response = glue.get_connection(Name=connection_id, HidePassword=False)
    if 'Connection' not in response:
        raise ConnectionNotFoundException(f'Connection [{connection_id}] could not be found.')
    cprops = response.get('Connection').get('ConnectionProperties')
    native_host = cprops.get('JDBC_CONNECTION_URL')[len('jdbc:'):]
    connection = Connection.parse_obj({
        'name': connection_id,
        'host': native_host,
        'principal': cprops.get('USERNAME'),
        'credential': cprops.get('PASSWORD'),
        'type': native_host.split(':')[0]
    })
    return connection
