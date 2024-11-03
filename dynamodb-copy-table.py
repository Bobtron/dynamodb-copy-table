import sys
import os
import json
import boto3

if len(sys.argv) != 3:
    print("Usage: %s <source_table_name> <destination_table_name>"% sys.argv[0])
    sys.exit(1)

src_table = sys.argv[1]
dst_table = sys.argv[2]
region = os.getenv('AWS_DEFAULT_REGION', 'us-west-2')
profile_name = os.getenv('PROFILE_NAME', None)

print(f'Using region: {region}')
print(f'Using profile name: {profile_name}')

boto_session = boto3.Session(profile_name=profile_name)
dynamodb_client = boto_session.client('dynamodb', region_name=region)

response = dynamodb_client.describe_table(
    TableName=src_table
)

source_table = response['Table']

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
    'OnDemandThroughput': {
        'MaxReadRequestUnits': '',
        'MaxWriteRequestUnits': ''
    }
}

print(json.dumps(source_table, indent=2, default=str))

def remove_unused_attr(attr_template, source_data):
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
        assert isinstance(source_data, str)
        return


remove_unused_attr(attr_to_keep, source_table)

dest_table = source_table

print(json.dumps(dest_table, indent=2, default=str))
