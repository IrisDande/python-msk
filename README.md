# AWS MSK Client Tool

A Python client for interacting with the AWS MSK (Managed Streaming for Kafka) cluster named "shared" using IAM authentication.

## Overview

This tool allows you to:
- List all Kafka topics in the MSK cluster
- Create new Kafka topics
- Produce messages to topics
- Consume messages from topics

The script uses IAM authentication to securely connect to the MSK cluster.


## Prerequisites

- Python 3.6+
- AWS CLI configured with appropriate credentials
- IAM permissions to access the MSK cluster

## Installation

### 1. Install Required Python Dependencies

```bash
# Using pip
pip install kafka-python boto3 aws-msk-iam-auth

# If you're using pip3 explicitly
pip3 install kafka-python boto3 aws-msk-iam-auth

# If you're working in a virtual environment
python -m pip install kafka-python boto3 aws-msk-iam-auth
```

### 2. Configure AWS Credentials

Ensure your AWS credentials are properly configured. The script uses the AWS profile `<profile_name>` by default.

```bash
# Verify your AWS credentials
aws sts get-caller-identity --profile <profile_name>
```

### 3. Make the Script Executable

```bash
chmod +x msk_client.py
```

## Usage

The script can be run in the following ways:

```bash
# Basic usage
python msk_client.py --action ACTION [OPTIONS]

# If the script is executable
./msk_client.py --action ACTION [OPTIONS]
```

### Available Actions

- `list_topics`: List all topics in the MSK cluster
- `create_topic`: Create a new topic
- `produce`: Send a message to a topic
- `consume`: Receive messages from a topic

### Common Options

- `--profile PROFILE`: AWS profile name (default: propertyguru-bds-marketplace-integration-BDSAdministratorAccess)
- `--region REGION`: AWS region (default: ap-southeast-1)
- `--topic TOPIC`: Kafka topic name (required for create_topic, produce, and consume actions)
- `--verbose`: Enable verbose logging

### Action-Specific Options

#### Create Topic Options
- `--partitions N`: Number of partitions (default: 1)
- `--replication-factor N`: Replication factor (default: 2)

#### Produce Options
- `--message MSG`: Message content (required, can be JSON or plain text)
- `--key KEY`: Optional message key

#### Consume Options
- `--group GROUP`: Consumer group ID (optional)
- `--timeout SECONDS`: Timeout in seconds (default: 30)

## Examples

### List All Topics

```bash
python msk_client.py --action list_topics
```

### Create a New Topic

```bash
python msk_client.py --action create_topic --topic my-new-topic --partitions 3 --replication-factor 2
```

### Produce a JSON Message

```bash
python msk_client.py --action produce --topic my-topic --message '{"key":"value","timestamp":"2023-01-01"}' --key "message-key"
```

### Produce a Plain Text Message

```bash
python msk_client.py --action produce --topic my-topic --message "Hello, Kafka!"
```

### Consume Messages

```bash
python msk_client.py --action consume --topic my-topic --timeout 60
```

### Consume Messages with a Specific Consumer Group

```bash
python msk_client.py --action consume --topic my-topic --group my-consumer-group
```

## AWS MSK IAM Authentication

This script uses AWS IAM authentication to connect to the MSK cluster. This approach:

1. Uses your AWS credentials to authenticate with the MSK cluster
2. Doesn't require storing passwords or SSL certificates
3. Leverages AWS IAM policies for fine-grained access control

The authentication flow works as follows:

1. The script creates an AWS session using your configured profile
2. It uses the `MSKAuthTokenProvider` from the aws-msk-iam-auth package to generate authentication tokens
3. The Kafka clients are configured with SASL_SSL security protocol and OAUTHBEARER mechanism
4. For each connection, a temporary token is generated and used for authentication

This approach is more secure than using passwords as it:
- Uses short-lived tokens rather than long-lived credentials
- Integrates with AWS IAM roles and policies
- Automatically rotates credentials

## Troubleshooting

### Common Issues

#### Authentication Issues

If you see errors like "Failed to get AWS credentials":

1. Verify your AWS credentials:
   ```bash
   aws sts get-caller-identity --profile <profile_name>
   ```

2. Ensure your IAM role has the necessary permissions:
   - `kafka:DescribeCluster`
   - `kafka:GetBootstrapBrokers`
   - `kafka:*Topic*` (for topic operations)

3. If using temporary credentials, they might have expired. Refresh them.

#### Connection Issues

If you see "No brokers available" or connection timeout errors:

1. Verify network connectivity to the MSK cluster
2. Check security groups allow traffic on port 9098 (SASL_SSL port)
3. Verify your VPC has proper connectivity to the MSK cluster

#### Topic Operation Failures

1. For topic creation failures:
   - Verify you have permissions to create topics
   - Check if the topic already exists
   - Ensure the topic name is valid (no special characters except `.`, `_`, and `-`)

2. For producer errors:
   - Check if the topic exists
   - Verify message size is not too large
   - Ensure JSON messages are correctly formatted

3. For consumer errors:
   - Check consumer group permissions
   - Verify the topic has messages
   - Ensure no other consumers in the same group have already consumed all messages

### Enabling Verbose Logging

For more detailed logging, use the `--verbose` flag:

```bash
python msk_client.py --action list_topics --verbose
```

## Additional Resources

- [AWS MSK Documentation](https://docs.aws.amazon.com/msk/)
- [Kafka Python Client Documentation](https://kafka-python.readthedocs.io/)
- [AWS MSK IAM Auth Package](https://github.com/aws/aws-msk-iam-auth)

