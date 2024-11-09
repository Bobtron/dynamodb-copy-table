import sys
import os
import json
import copy
import time
import boto3
from botocore.exceptions import ClientError


# JSON 'template' of attributes to be used in createTable call from describeTable response
attr_to_keep = {
    'AttributeDefinitions': [
        {
            'AttributeName': '',
            'AttributeType': ''
        }
    ],
    'KeySchema': [
        {
            'AttributeName': '',
            'KeyType': ''
        }
    ],
    'LocalSecondaryIndexes': [
        {
            'IndexName': '',
            'KeySchema': [
                {
                    'AttributeName': '',
                    'KeyType': ''
                }
            ],
            'Projection': {
                'ProjectionType': '',
                'NonKeyAttributes': ['']
            }
        }
    ],
    'GlobalSecondaryIndexes': [
        {
            'IndexName': '',
            'KeySchema': [
                {
                    'AttributeName': '',
                    'KeyType': ''
                },
            ],
            'Projection': {
                'ProjectionType': '',
                'NonKeyAttributes': ['']
            },
            'ProvisionedThroughput': {
                'ReadCapacityUnits': '',
                'WriteCapacityUnits': ''
            },
            'OnDemandThroughput': {
                'MaxReadRequestUnits': '',
                'MaxWriteRequestUnits': ''
            }
        },
    ],
    'ProvisionedThroughput': {
        'ReadCapacityUnits': '',
        'WriteCapacityUnits': ''
    },
    'StreamSpecification': {
        'StreamEnabled': '',
        'StreamViewType': ''
    },
    'DeletionProtectionEnabled': '',
    'OnDemandThroughput': {
        'MaxReadRequestUnits': '',
        'MaxWriteRequestUnits': ''
    }
}

def remove_unused_attr(attr_template, source_data):
    """Removes attributes found in source_data that do not exist in the template"""
    if isinstance(attr_template, dict):
        assert isinstance(source_data, dict)
        curr_keys = list(source_data.keys())
        for key in curr_keys:
            if key not in attr_template.keys():
                del source_data[key]
            else:
                remove_unused_attr(attr_template[key], source_data[key])
    elif isinstance(attr_template, list):
        assert isinstance(source_data, list)
        for item in source_data:
            remove_unused_attr(attr_template[0], item)
    elif isinstance(attr_template, str):
        assert isinstance(source_data, (str, bool, int))
        # TODO: check if the values in source_data are valid according to
        # the values accepted by boto3 API

def create_dst_table(dynamodb_client, src_table_name, dst_table_name):
    """Creates the destination table if applicable"""
    response = None
    try:
        response = dynamodb_client.describe_table(
            TableName=src_table_name
        )
    except ClientError as err:
        if err.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f'Source table named {src_table_name} not found')
        else:
            print(f'Unknown exception {err} encountered')
        raise err

    try:
        dst_response = dynamodb_client.describe_table(
            TableName=dst_table_name
        )
        dst_table_status = dst_response['Table']['TableStatus']
        assert dst_table_status == 'ACTIVE', \
            f'Destination table exists but TableStatus={dst_table_status} is not ACTIVE'
        print(f'Destination table named {dst_table_name} already exists, skipping creation')
        return
    except ClientError as err:
        if err.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f'Destination table named {dst_table_name} not found, creating...')
        else:
            print(f'Unknown exception {err} encountered')
            raise err

    src_table = response['Table']
    dst_table = copy.deepcopy(src_table)

    assert src_table['TableStatus'] == 'ACTIVE', \
        f'Source table TableStatus={src_table["TableStatus"]} is not ACTIVE'

    # print(json.dumps(src_table, indent=2, default=str))

    # Create the new destination table with same attributes from old table except the table name
    remove_unused_attr(attr_to_keep, dst_table)

    # Other attributes to be added to dst_table that are different from DescribeTable response
    if 'BillingModeSummary' in src_table.keys():
        dst_table['BillingMode'] = src_table['BillingModeSummary']['BillingMode']
    else:
        if 'OnDemandThroughput' in dst_table.keys():
            dst_table['BillingMode'] = 'PAY_PER_REQUEST'
        else:
            # Defaults to PROVISIONED if no BillingMode in source table and
            # no OnDemandThroughput specified
            dst_table['BillingMode'] = 'PROVISIONED'
    if 'TableClassSummary' in src_table.keys():
        dst_table['TableClass'] = src_table['TableClassSummary']['TableClass']
    else: # Defaults to STANDARD
        dst_table['TableClass'] = 'STANDARD'
    dst_table['TableName'] = dst_table_name

    # print(json.dumps(dst_table, indent=2, default=str))

    dynamodb_client.create_table(**dst_table)
    time.sleep(5)
    while True:
        try:
            dst_response = dynamodb_client.describe_table(
                TableName=dst_table_name
            )
            dst_table_status = dst_response['Table']['TableStatus']
            assert dst_table_status in {'ACTIVE', 'CREATING'}, \
                f'Destination table TableStatus={dst_table_status} is invalid'
            if dst_table_status == 'ACTIVE':
                print(f'Destination table {dst_table_name} created')
                break
            print(f'Destination table {dst_table_name} creating...')
            time.sleep(3)
        except ClientError as err:
            print(f'Unknown exception {err} encountered')
            raise err

def copy_from_src_to_dst(dynamodb_client, src_table_name, dst_table_name):
    """Copies the items from src table to dst table"""
    # TODO: Error handling, especially ProvisionedThroughputExceededException

    print(f'Initial scan for items in {src_table_name} table')
    try:
        src_scan_response = dynamodb_client.scan(
            TableName=src_table_name,
            Select='ALL_ATTRIBUTES'
        )
    except ClientError as err:
        print(f'Unknown exception {err} encountered')
        raise err

    while 'Items' in src_scan_response.keys():
        items = src_scan_response['Items']

        print(f'Put items from scan into {dst_table_name} table')
        for item in items:
            try: 
                dynamodb_client.put_item(
                    TableName=dst_table_name,
                    Item=item
                )
            except ClientError as err:
                print(f'Unknown exception {err} encountered')
                raise err

        if 'LastEvaluatedKey' not in src_scan_response.keys():
            print(f'Copying from {src_table_name} table to {dst_table_name} table completed')
            break
        exclusive_start_key = src_scan_response['LastEvaluatedKey']

        print(f'Scan for items in {src_table_name} table with exclusive start key {exclusive_start_key}')

        try:
            src_scan_response = dynamodb_client.scan(
                TableName=src_table_name,
                Select='ALL_ATTRIBUTES',
                ExclusiveStartKey=exclusive_start_key
            )
        except ClientError as err:
            print(f'Unknown exception {err} encountered')
            raise err

def main():
    """Entrypoint"""

    def getenv(var_name, default=None):
        """Look for an environment variable in a case-insensitive way"""
        for key, value in os.environ.items():
            if key.lower() == var_name.lower():
                return value
        return default

    def env_exists(var_name):
        """Return if environmental variable exists"""
        for key in os.environ.keys():
            if key.lower() == var_name.lower():
                return True
        return False

    src_table_name = sys.argv[1]
    dst_table_name = sys.argv[2]
    REGION = getenv('AWS_DEFAULT_REGION', 'us-west-2')
    PROFILE_NAME = getenv('PROFILE_NAME', None)
    AWS_ACCESS_KEY = getenv('AWS_ACCESS_KEY_ID', None)
    AWS_SECRET_ACCESS_KEY = getenv('AWS_SECRET_ACCESS_KEY', None)
    DISABLE_CREATION = env_exists('DISABLE_CREATION')
    DISABLE_DATACOPY = env_exists('DISABLE_DATACOPY')

    boto3_session = None

    print(f'Using region: {REGION}')
    if PROFILE_NAME is not None:
        print(f'Using profile name: {PROFILE_NAME}')
        boto3_session = boto3.Session(profile_name=PROFILE_NAME)
    elif AWS_ACCESS_KEY is not None and AWS_SECRET_ACCESS_KEY is not None:
        print('Using AWS_ACCESS_KEY and AWS_SECRET_ACCESS_KEY')
        boto3_session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    else:
        print(f'Using default credentials')
        boto3_session = boto3.Session()

    dynamodb_client = boto3_session.client('dynamodb', region_name=REGION)

    if not DISABLE_CREATION:
        create_dst_table(dynamodb_client, src_table_name, dst_table_name)

    if not DISABLE_DATACOPY:
        copy_from_src_to_dst(dynamodb_client, src_table_name, dst_table_name)

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(f'Usage: {sys.argv[0]} <source_table_name> <destination_table_name>')
        sys.exit(1)

    main()
