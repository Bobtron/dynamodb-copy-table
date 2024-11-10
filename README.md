# dynamodb-copy-table
A simple python 3 script to copy dynamodb table

#### Inspired by [techgaun's dynamodb-copy-table](https://github.com/techgaun/dynamodb-copy-table)

New features:
* Uses boto3 instead of legacy [boto](https://github.com/boto/boto?tab=readme-ov-file#deprecation-notice)
* Copies over source table configs and indexes .etc, when creating destination table
* Users can specify profile name for credentials

---

### Requirements

- Python >=3.8
- boto3 (`pip install boto3`)

### Usage

A simple usage example:

```shell
$ python dynamodb-copy-table.py src_table dst_table
```

The following environment variables can be used:
Variable | Purpose
--- | ---
`AWS_DEFAULT_REGION` | Select the region (the default region is `us-west-2`)
`DISABLE_CREATION` | Disables creation of a new table (Useful if the table already exists)
`DISABLE_DATACOPY` | Disables copying of data from source table to destination table
`PROFILE_NAME` | Name of AWS profile to be used for credentials (will use default profile if this and AWS key are not specified)
`AWS_ACCESS_KEY_ID` | AWS credentials to be used if profile is not specified
`AWS_SECRET_ACCESS_KEY` | AWS credentials to be used if profile is not specified

```shell
$ AWS_DEFAULT_REGION=us-east-1 DISABLE_CREATION=yes DISABLE_DATACOPY=yes \
python dynamodb-copy-table.py src_table dst_table
```

```shell
$ PROFILE_NAME=MyServiceUser \
python dynamodb-copy-table.py src_table dst_table
```

### Docker Image

The docker image is available as [fiodel/dynamodb-copy-table:latest](https://hub.docker.com/r/fiodel/dynamodb-copy-table)
in the official docker hub that you can pull from.

Docker Usage:

```shell
# pull image down
docker pull fiodel/dynamodb-copy-table:latest

# invoke help
$ docker run --rm -it fiodel/dynamodb-copy-table:latest
Usage: dynamodb-copy-table.py <source_table_name> <destination_table_name>

# invoke copy
docker run -e AWS_ACCESS_KEY_ID=foo -e AWS_SECRET_ACCESS_KEY=bar --rm -it fiodel/dynamodb-copy-table:latest src_table dst_table
```

### References

- [DynamoDB Boto3 Client reference](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html)
