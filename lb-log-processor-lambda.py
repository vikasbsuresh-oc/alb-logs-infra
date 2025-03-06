import json
import boto3
import gzip
import os
import datetime
import logging
import io

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Clients
s3_client = boto3.client('s3')
logs_client = boto3.client('logs')
sqs_client = boto3.client('sqs')

# Environment variables
DLQ_QUEUE_URL = os.environ['DLQ_QUEUE_URL']
LOG_GROUP_NAME = os.environ['LOG_GROUP_NAME']
LOG_STREAM_NAME = "alb-logs-" + datetime.datetime.utcnow().strftime("%Y-%m-%d")
MAX_RETRY_COUNT = 3  # Max retries before DLQ

def parse_s3_event(record):
    """Extract bucket name and object key from S3 event inside SQS."""
    s3_info = json.loads(record['body'])['Records'][0]['s3']
    bucket_name = s3_info['bucket']['name']
    object_key = s3_info['object']['key']
    return bucket_name, object_key

def fetch_alb_log(bucket, key):
    """Retrieve and decompress ALB log file from S3."""
    logger.info(f"Fetching ALB log from S3://{bucket}/{key}")
    
    response = s3_client.get_object(Bucket=bucket, Key=key)
    compressed_data = response['Body'].read()  # Returns bytes

    # Wrap bytes in a BytesIO object before passing to gzip
    with gzip.GzipFile(fileobj=io.BytesIO(compressed_data), mode='rb') as f:
        log_data = f.read().decode('utf-8')  # Decode bytes to string

    return log_data

def write_to_cloudwatch(log_data):
    """Write logs to CloudWatch Log Stream."""
    logger.info(f"Writing logs to CloudWatch: {LOG_GROUP_NAME}/{LOG_STREAM_NAME}")

    try:
        logs_client.create_log_group(logGroupName=LOG_GROUP_NAME)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass

    try:
        logs_client.create_log_stream(logGroupName=LOG_GROUP_NAME, logStreamName=LOG_STREAM_NAME)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        pass

    # Get the sequence token for the existing log stream
    response = logs_client.describe_log_streams(
        logGroupName=LOG_GROUP_NAME,
        logStreamNamePrefix=LOG_STREAM_NAME
    )
    log_streams = response.get('logStreams', [])
    
    # If log streams exist, get the sequence token
    sequence_token = log_streams[0].get('uploadSequenceToken') if log_streams else None

    # Prepare log events (timestamp in milliseconds and message)
    log_events = [{'timestamp': int(datetime.datetime.utcnow().timestamp() * 1000), 'message': line}
                  for line in log_data.split("\n") if line]

    # If sequence token is available, add it to the request
    put_log_events_params = {
        'logGroupName': LOG_GROUP_NAME,
        'logStreamName': LOG_STREAM_NAME,
        'logEvents': log_events
    }

    if sequence_token:
        put_log_events_params['sequenceToken'] = sequence_token

    # Send logs to CloudWatch
    try:
        logs_client.put_log_events(**put_log_events_params)
        logger.info(f"Successfully sent logs to CloudWatch log stream: {LOG_STREAM_NAME}")
    except Exception as e:
        logger.error(f"Failed to send logs to CloudWatch: {e}", exc_info=True)


def move_to_dlq(message):
    """Move failed messages to Dead Letter Queue."""
    logger.warning(f"Moving message {message['messageId']} to DLQ.")
    sqs_client.send_message(
        QueueUrl=DLQ_QUEUE_URL,
        MessageBody=message['body']
    )

def lambda_handler(event, context):
    """Triggered by SQS."""
    logger.info(f"Received {len(event['Records'])} messages from SQS.")

    for record in event['Records']:
        try:
            receive_count = int(record['attributes'].get('ApproximateReceiveCount', 1))

            if receive_count > MAX_RETRY_COUNT:
                logger.warning(f"Message {record['messageId']} exceeded retry limit. Sending to DLQ.")
                move_to_dlq(record)
                continue

            bucket, key = parse_s3_event(record)
            logger.info(f"Processing ALB log from S3://{bucket}/{key}")

            log_data = fetch_alb_log(bucket, key)
            write_to_cloudwatch(log_data)

            logger.info(f"Message {record['messageId']} processed successfully.")

        except Exception as e:
            logger.error(f"Error processing message {record['messageId']}: {e}", exc_info=True)

    return {"status": "success"}
