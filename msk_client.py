#!/usr/bin/env python3
"""
MSK Client - A tool for interacting with AWS MSK (Managed Streaming for Kafka)

This script provides functionality to interact with an AWS MSK cluster using IAM authentication.
It supports listing topics, creating topics, producing messages to topics, and consuming messages
from topics.

Usage:
    python msk_client.py --action list_topics
    python msk_client.py --action create_topic --topic my-topic
    python msk_client.py --action produce --topic my-topic --message "Hello, Kafka!"
    python msk_client.py --action consume --topic my-topic [--group my-group]

Requirements:
    - Python 3.6+
    - Required packages: kafka-python, boto3, aws-msk-iam-auth
    - IAM permissions to access the MSK cluster
"""

import os
import sys
import json
import time
import logging
import argparse
import ssl
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Any

try:
    import boto3
    from botocore.exceptions import ClientError, ProfileNotFound
    from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import KafkaError, NoBrokersAvailable
    from kafka.sasl.oauth import AbstractTokenProvider
    from aws_msk_iam_sasl_signer.MSKAuthTokenProvider import generate_auth_token_from_profile
except ImportError as e:
    print(f"Error: Required package not found - {e}")
    print("Please install required packages:")
    print("pip install kafka-python boto3 aws-msk-iam-auth")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('msk_client')


class MSKClientError(Exception):
    """Base exception for MSK client errors"""
    pass


class AuthenticationError(MSKClientError):
    """Raised when there's an issue with AWS authentication"""
    pass


class ConnectionError(MSKClientError):
    """Raised when there's an issue connecting to the MSK cluster"""
    pass


class TopicError(MSKClientError):
    """Raised when there's an issue with topic operations"""
    pass


class MSKTokenProvider(AbstractTokenProvider):
    """
    MSK IAM token provider for SASL/OAUTHBEARER authentication

    Implements the AbstractTokenProvider interface required by kafka-python
    for SASL/OAUTHBEARER authentication.
    """

    def __init__(self, region, aws_profile):
        """
        Initialize the token provider

        Args:
            region: AWS region where the MSK cluster is located
            aws_profile: AWS profile name to use for authentication
        """
        self.region = region
        self.aws_profile = aws_profile
        self._token_value = None
        self.expiry = 0
        self._refresh_token()

    def _refresh_token(self):
        """Generate a new token using the MSK IAM auth mechanism"""
        try:
            token, expiry_ms = generate_auth_token_from_profile(
                region=self.region,
                aws_profile=self.aws_profile
            )
            self._token_value = token
            self.expiry = expiry_ms
        except Exception as e:
            raise AuthenticationError(
                f"Failed to generate MSK auth token: {str(e)}")

    def token(self):
        """
        Get the current token, refreshing if needed

        Returns:
            Token information in the format required by SASL/OAUTHBEARER
        """
        current_time_ms = int(time.time() * 1000)

        # If token is expired or close to expiry (within 5 minutes), refresh it
        # 5 minutes in ms
        if not self._token_value or current_time_ms > (self.expiry - 300000):
            self._refresh_token()

        # Return the raw token string as required by kafka-python's OAUTHBEARER mechanism
        return self._token_value


class MSKClient:
    """
    Client for interacting with AWS MSK (Managed Streaming for Kafka)

    This class provides methods to connect to an MSK cluster using IAM authentication
    and perform various Kafka operations like listing topics, creating topics, 
    producing messages, and consuming messages.
    """

    def __init__(self, profile_name: str = DEFAULT_PROFILE, region: str = DEFAULT_REGION):
        """
        Initialize the MSK client

        Args:
            profile_name: AWS profile name to use for authentication
            region: AWS region where the MSK cluster is located
        """
        self.profile_name = profile_name
        self.region = region
        self.bootstrap_servers = BOOTSTRAP_SERVERS
        self.aws_session = None
        self.aws_session = None
        self.token_provider = None  # Will hold an instance of MSKTokenProvider
        self.admin_client = None
        self.consumer = None

        self._setup_aws_session()
        self._setup_token_provider()

    def _setup_aws_session(self) -> None:
        """Set up AWS session with credentials from the specified profile"""
        try:
            logger.info(
                f"Setting up AWS session with profile: {self.profile_name}")
            self.aws_session = boto3.Session(profile_name=self.profile_name)
            credentials = self.aws_session.get_credentials()
            if not credentials:
                raise AuthenticationError("Failed to get AWS credentials")

            # Validate credentials by making a simple API call
            client = self.aws_session.client('sts')
            client.get_caller_identity()
            logger.info("AWS credentials validated successfully")

        except ProfileNotFound:
            raise AuthenticationError(
                f"AWS profile '{self.profile_name}' not found")
        except ClientError as e:
            raise AuthenticationError(f"AWS authentication error: {str(e)}")

    def _setup_token_provider(self) -> None:
        """Set up MSK IAM authentication token provider"""
        try:
            logger.info("Setting up MSK IAM authentication token provider")
            # Create a token provider instance that implements AbstractTokenProvider
            self.token_provider = MSKTokenProvider(
                region=self.region,
                aws_profile=self.profile_name
            )
            logger.info("MSK IAM token provider successfully created")
        except Exception as e:
            raise AuthenticationError(
                f"Failed to set up MSK IAM token provider: {str(e)}")

    def _get_auth_config(self) -> Dict[str, Any]:
        """
        Get authentication configuration for Kafka clients

        Returns:
            Dict containing authentication configuration
        """
        return {
            'security_protocol': 'SASL_SSL',
            'sasl_mechanism': 'OAUTHBEARER',
            # Use our custom token provider that implements AbstractTokenProvider
            'sasl_oauth_token_provider': self.token_provider,
            # SSL/TLS configuration
            'ssl_check_hostname': True,
            'ssl_context': ssl.create_default_context()
        }

    def _create_admin_client(self) -> KafkaAdminClient:
        """
        Create a Kafka admin client

        Returns:
            KafkaAdminClient instance

        Raises:
            ConnectionError: If connection to MSK cluster fails
        """
        if self.admin_client:
            return self.admin_client

        try:
            logger.info("Creating Kafka admin client")
            config = self._get_auth_config()

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    self.admin_client = KafkaAdminClient(
                        bootstrap_servers=self.bootstrap_servers,
                        client_id=f'msk-admin-client-{int(time.time())}',
                        **config
                    )
                    logger.info("Successfully created Kafka admin client")
                    return self.admin_client
                except NoBrokersAvailable:
                    if attempt == MAX_RETRIES:
                        raise
                    logger.warning(
                        f"No brokers available, retrying ({attempt}/{MAX_RETRIES})...")
                    time.sleep(RETRY_BACKOFF * attempt)

        except KafkaError as e:
            raise ConnectionError(
                f"Failed to connect to MSK cluster: {str(e)}")

    def _create_producer(self) -> KafkaProducer:
        """
        Create a Kafka producer

        Returns:
            KafkaProducer instance

        Raises:
            ConnectionError: If connection to MSK cluster fails
        """
        if self.producer:
            return self.producer

        try:
            logger.info("Creating Kafka producer")
            config = self._get_auth_config()

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    self.producer = KafkaProducer(
                        bootstrap_servers=self.bootstrap_servers,
                        value_serializer=lambda v: json.dumps(
                            v).encode('utf-8'),
                        key_serializer=lambda k: k.encode(
                            'utf-8') if k else None,
                        acks='all',  # Wait for all replicas to acknowledge
                        retries=3,   # Retry sending messages
                        **config
                    )
                    logger.info("Successfully created Kafka producer")
                    return self.producer
                except NoBrokersAvailable:
                    if attempt == MAX_RETRIES:
                        raise
                    logger.warning(
                        f"No brokers available, retrying ({attempt}/{MAX_RETRIES})...")
                    time.sleep(RETRY_BACKOFF * attempt)

        except KafkaError as e:
            raise ConnectionError(f"Failed to create Kafka producer: {str(e)}")

    def _create_consumer(self, topic: str, group_id: str = None, auto_offset_reset: str = 'earliest') -> KafkaConsumer:
        """
        Create a Kafka consumer

        Args:
            topic: Topic to consume messages from
            group_id: Consumer group ID (optional)
            auto_offset_reset: Where to start consuming from if no offset is stored

        Returns:
            KafkaConsumer instance

        Raises:
            ConnectionError: If connection to MSK cluster fails
        """
        try:
            logger.info(f"Creating Kafka consumer for topic: {topic}")
            config = self._get_auth_config()

            # If group_id is not provided, create a random one
            if not group_id:
                group_id = f'msk-consumer-{int(time.time())}'

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    consumer = KafkaConsumer(
                        topic,
                        bootstrap_servers=self.bootstrap_servers,
                        group_id=group_id,
                        auto_offset_reset=auto_offset_reset,
                        enable_auto_commit=True,
                        value_deserializer=lambda v: json.loads(
                            v.decode('utf-8')),
                        **config
                    )
                    logger.info(
                        f"Successfully created Kafka consumer for topic: {topic}")
                    return consumer
                except NoBrokersAvailable:
                    if attempt == MAX_RETRIES:
                        raise
                    logger.warning(
                        f"No brokers available, retrying ({attempt}/{MAX_RETRIES})...")
                    time.sleep(RETRY_BACKOFF * attempt)

        except KafkaError as e:
            raise ConnectionError(f"Failed to create Kafka consumer: {str(e)}")

    def list_topics(self) -> List[str]:
        """
        List all topics in the MSK cluster

        Returns:
            List of topic names

        Raises:
            ConnectionError: If connection to MSK cluster fails
        """
        try:
            admin_client = self._create_admin_client()
            logger.info("Listing Kafka topics")
            topics = admin_client.list_topics()
            logger.info(f"Found {len(topics)} topics")
            return topics
        except KafkaError as e:
            raise ConnectionError(f"Failed to list topics: {str(e)}")

    def create_topic(self, topic_name: str, num_partitions: int = DEFAULT_NUM_PARTITIONS,
                     replication_factor: int = DEFAULT_REPLICATION_FACTOR) -> None:
        """
        Create a new topic in the MSK cluster

        Args:
            topic_name: Name of the topic to create
            num_partitions: Number of partitions for the topic
            replication_factor: Replication factor for the topic

        Raises:
            TopicError: If topic creation fails
            ConnectionError: If connection to MSK cluster fails
        """
        try:
            admin_client = self._create_admin_client()

            # Check if topic already exists
            existing_topics = admin_client.list_topics()
            if topic_name in existing_topics:
                logger.warning(f"Topic '{topic_name}' already exists")
                return

            logger.info(
                f"Creating topic '{topic_name}' with {num_partitions} partitions and replication factor {replication_factor}")

            # Define topic configuration
            topic_configs = {
                'retention.ms': str(DEFAULT_RETENTION_MS),
                'cleanup.policy': 'delete',
                'min.insync.replicas': '1'
            }

            # Create the topic
            new_topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                topic_configs=topic_configs
            )

            admin_client.create_topics([new_topic])
            logger.info(f"Successfully created topic '{topic_name}'")

        except KafkaError as e:
            raise TopicError(
                f"Failed to create topic '{topic_name}': {str(e)}")

    def produce_message(self, topic: str, message: Any, key: str = None) -> None:
        """
        Produce a message to a Kafka topic

        Args:
            topic: Name of the topic to produce to
            message: Message to produce (will be serialized to JSON)
            key: Optional message key

        Raises:
            ConnectionError: If connection to MSK cluster fails
        """
        try:
            producer = self._create_producer()

            logger.info(f"Sending message to topic '{topic}'")
            future = producer.send(topic, value=message, key=key)
            result = future.get(timeout=10)  # Wait for the message to be sent

            logger.info(
                f"Message sent to topic '{topic}', partition: {result.partition}, offset: {result.offset}")
            producer.flush()  # Ensure all messages are sent

        except KafkaError as e:
            raise ConnectionError(
                f"Failed to produce message to topic '{topic}': {str(e)}")

    def consume_messages(self, topic: str, group_id: str = None, timeout: int = 30, max_messages: int = 100) -> List[Dict]:
        """
        Consume messages from a Kafka topic

        Args:
            topic: Name of the topic to consume from
            group_id: Consumer group ID (optional)
            timeout: Timeout in seconds for consuming messages
            max_messages: Maximum number of messages to consume

        Returns:
            List of consumed messages

        Raises:
            ConnectionError: If connection to MSK cluster fails
        """
        try:
            consumer = self._create_consumer(topic, group_id)

            logger.info(f"Starting to consume messages from topic '{topic}'")
            messages = []
            stop_event = threading.Event()

            # Set a timeout to stop consuming
            def stop_after_timeout():
                time.sleep(timeout)
                logger.info(f"Reached timeout of {timeout} seconds")
                stop_event.set()

            # Start timeout thread
            timeout_thread = threading.Thread(target=stop_after_timeout)
            timeout_thread.daemon = True
            timeout_thread.start()

            # Start consuming messages
            count = 0
            try:
                logger.info("Waiting for messages...")
                for message in consumer:
                    if stop_event.is_set() or count >= max_messages:
                        break

                    msg_data = {
                        'topic': message.topic,
                        'partition': message.partition,
                        'offset': message.offset,
                        'key': message.key.decode('utf-8') if message.key else None,
                        'value': message.value,
                        'timestamp': message.timestamp
                    }
                    messages.append(msg_data)
                    logger.info(f"Received message: {msg_data}")
                    count += 1
            finally:
                stop_event.set()
                consumer.close()
                logger.info(
                    f"Consumed {len(messages)} messages from topic '{topic}'")

            return messages

        except KafkaError as e:
            raise ConnectionError(
                f"Failed to consume messages from topic '{topic}': {str(e)}")


def parse_args():
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(
        description='AWS MSK (Managed Streaming for Kafka) Client')

    parser.add_argument('--profile', type=str, default=DEFAULT_PROFILE,
                        help='AWS profile name for authentication')

    parser.add_argument('--region', type=str, default=DEFAULT_REGION,
                        help='AWS region where the MSK cluster is located')

    parser.add_argument('--action', type=str, required=True,
                        choices=['list_topics', 'create_topic',
                                 'produce', 'consume'],
                        help='Action to perform')

    parser.add_argument('--topic', type=str,
                        help='Topic name for create_topic, produce, and consume actions')

    parser.add_argument('--message', type=str,
                        help='Message content for produce action (JSON string or plain text)')

    parser.add_argument('--key', type=str,
                        help='Message key for produce action')

    parser.add_argument('--group', type=str,
                        help='Consumer group ID for consume action')

    parser.add_argument('--partitions', type=int, default=DEFAULT_NUM_PARTITIONS,
                        help='Number of partitions for create_topic action')

    parser.add_argument('--replication-factor', type=int, default=DEFAULT_REPLICATION_FACTOR,
                        help='Replication factor for create_topic action')

    parser.add_argument('--timeout', type=int, default=30,
                        help='Timeout in seconds for consume action')

    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose logging')

    # Add new command-line arguments for configuration
    parser.add_argument('--cluster-arn', type=str, default=CLUSTER_ARN,
                        help='ARN of the MSK cluster')

    parser.add_argument('--bootstrap-servers', type=str, nargs='+',
                        default=BOOTSTRAP_SERVERS,
                        help='List of bootstrap servers (space-separated)')

    parser.add_argument('--retention-ms', type=int, default=DEFAULT_RETENTION_MS,
                        help='Message retention period in milliseconds')

    parser.add_argument('--max-retries', type=int, default=MAX_RETRIES,
                        help='Maximum number of connection retry attempts')

    parser.add_argument('--retry-backoff', type=float, default=RETRY_BACKOFF,
                        help='Backoff time between retries in seconds')

    args = parser.parse_args()

    # Update global configuration values from command line arguments
    global DEFAULT_PROFILE, DEFAULT_REGION, CLUSTER_ARN, BOOTSTRAP_SERVERS
    global DEFAULT_NUM_PARTITIONS, DEFAULT_REPLICATION_FACTOR, DEFAULT_RETENTION_MS
    global MAX_RETRIES, RETRY_BACKOFF

    DEFAULT_PROFILE = args.profile
    DEFAULT_REGION = args.region
    CLUSTER_ARN = args.cluster_arn
    BOOTSTRAP_SERVERS = args.bootstrap_servers
    DEFAULT_RETENTION_MS = args.retention_ms
    MAX_RETRIES = args.max_retries
    RETRY_BACKOFF = args.retry_backoff

    return args


def validate_args(args):
    """Validate command line arguments"""
    if args.action in ['create_topic', 'produce', 'consume'] and not args.topic:
        raise ValueError(f"--topic is required for {args.action} action")

    if args.action == 'produce' and not args.message:
        raise ValueError("--message is required for produce action")


def format_message(message_str):
    """Convert message string to object if it's valid JSON, otherwise return as is"""
    try:
        return json.loads(message_str)
    except json.JSONDecodeError:
        return message_str


def main():
    """Main entry point"""
    try:
        args = parse_args()

        if args.verbose:
            logging.getLogger('msk_client').setLevel(logging.DEBUG)

        validate_args(args)

        # Create MSK client
        client = MSKClient(profile_name=args.profile, region=args.region)

        # Execute action
        if args.action == 'list_topics':
            topics = client.list_topics()
            print("\nAvailable Topics:")
            if topics:
                for i, topic in enumerate(topics, 1):
                    print(f"{i}. {topic}")
            else:
                print("No topics found")

        elif args.action == 'create_topic':
            client.create_topic(
                topic_name=args.topic,
                num_partitions=args.partitions,
                replication_factor=args.replication_factor
            )
            print(f"\nTopic '{args.topic}' created successfully")

        elif args.action == 'produce':
            message = format_message(args.message)
            client.produce_message(
                topic=args.topic,
                message=message,
                key=args.key
            )
            print(f"\nMessage sent to topic '{args.topic}' successfully")

        elif args.action == 'consume':
            print(f"\nConsuming messages from topic '{args.topic}'...")
            print(
                f"Press Ctrl+C to stop consuming (or wait for {args.timeout}s timeout)")
            messages = client.consume_messages(
                topic=args.topic,
                group_id=args.group,
                timeout=args.timeout
            )

            if messages:
                print(f"\nReceived {len(messages)} messages:")
                for i, msg in enumerate(messages, 1):
                    print(f"\n--- Message {i} ---")
                    print(f"Topic: {msg['topic']}")
                    print(f"Partition: {msg['partition']}")
                    print(f"Offset: {msg['offset']}")
                    if msg['key']:
                        print(f"Key: {msg['key']}")
                    print(f"Value: {json.dumps(msg['value'], indent=2)}")
            else:
                print("\nNo messages received")

    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
        sys.exit(0)
    except MSKClientError as e:
        print(f"\nError: {str(e)}")
        sys.exit(1)
    except ValueError as e:
        print(f"\nError: {str(e)}")
        print("Run with --help for usage information")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    print("""
╔════════════════════════════════════════════════════════════╗
║                AWS MSK Kafka Client Tool                   ║
╚════════════════════════════════════════════════════════════╝
""")
    main()


"""
TROUBLESHOOTING GUIDE
=====================

Authentication Issues:
---------------------
1. Ensure your AWS profile is correctly configured:
   Run 'aws configure list --profile <profile-name>' to check

2. Verify you have the necessary IAM permissions:
   You need permissions for 'kafka:*' actions on the MSK cluster
   Your role should be added to the MSK cluster's access control policy

3. Check for expired credentials:
   If using temporary credentials, they might have expired

Connection Issues:
------------------
1. Verify network connectivity to the MSK cluster:
   Ensure your VPC has proper connectivity to the MSK cluster

2. Check security groups:
   Make sure your security groups allow traffic on ports 9098 for SASL_SSL

3. SSL/TLS issues:
   Verify that the SSL context is correctly configured
   Try setting 'ssl_check_hostname=False' for testing (not recommended for production)

Topic Operations:
----------------
1. Topic creation failures:
   - Verify you have permissions to create topics
   - Check if auto.create.topics.enable is set to false on the cluster
   - Ensure the topic name follows the Kafka naming rules

2. Producer errors:
   - Check if the topic exists
   - Verify message size is not exceeding the broker limit
   - Ensure the message format is correct (valid JSON)

3. Consumer errors:
   - Check consumer group permissions
   - Verify the topic has messages to consume
   - Check if other consumers in the same group already consumed all messages

Common Error Codes:
------------------
1. 'NoBrokersAvailable': 
   - Network connectivity issue
   - Wrong bootstrap server addresses
   - Authentication failure

2. 'TopicAuthorizationFailed': 
   - Missing IAM permissions for the topic

3. 'NotLeaderForPartition': 
   - Broker leadership has changed, retry the operation

For more help:
-------------
- AWS MSK Documentation: https://docs.aws.amazon.com/msk/
- kafka-python Documentation: https://kafka-python.readthedocs.io/
- aws-msk-iam-auth: https://github.com/aws/aws-msk-iam-auth
"""
