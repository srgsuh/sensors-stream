import os
import json
import boto3
from helpers.logs import get_logger

logger = get_logger(__name__)
sns_client = boto3.client('sns')

def lambda_handler(event, context):
    """
    Lambda handler for detecting abnormal sensor data.
    Subscribes to sns-sensors-ingress and publishes to sns-sensors-abnormal.
    """
    try:
        logger.debug("EVENT: %s", event)
        
        sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
        if not sns_topic_arn:
            raise ValueError("SNS_TOPIC_ARN environment variable not set")
        
        # Process each SNS record
        for record in event.get('Records', []):
            if record.get('EventSource') == 'aws:sns':
                message = record['Sns']['Message']
                logger.info("Processing message: %s", message)
                
                # Parse the message (assuming it's JSON)
                try:
                    sensor_data = json.loads(message)
                except json.JSONDecodeError:
                    logger.warning("Message is not valid JSON, using raw message")
                    sensor_data = message
                
                # Detect abnormalities (placeholder logic)
                # TODO: Implement actual abnormality detection logic
                abnormal_data = {
                    'type': 'abnormal',
                    'original_data': sensor_data,
                    'processed_by': 'sensors-abnormal-lambda',
                    'anomaly_detected': True
                }
                
                # Publish to sns-sensors-abnormal
                response = sns_client.publish(
                    TopicArn=sns_topic_arn,
                    Message=json.dumps(abnormal_data),
                    Subject='Abnormal Sensor Data Detected'
                )
                
                logger.info("Published to SNS topic %s: %s", sns_topic_arn, response['MessageId'])
        
        return {
            'statusCode': 200,
            'body': json.dumps('Successfully processed sensor data')
        }
        
    except Exception as e:
        logger.error("Error processing sensor data: %s", e)
        raise
