import os
import json
from helpers.logs import get_logger
from helpers.config import get_env_var
from helpers.sns_common import sns_client
from helpers.dynamo_db import get_sensor_parameters

logger = get_logger("an-lambda")

_sensor_limits: dict[str, tuple[int, int]] = {}

def get_sensor_limits(sensor_id: str) -> tuple[int, int]:
    if sensor_id not in _sensor_limits:
        params = get_sensor_parameters(sensor_id)
        if not params:
            raise ValueError(f"Sensor bounds not found for sensor_id: {sensor_id}")
        _sensor_limits[sensor_id] = (params["min_value"], params["max_value"])
    return _sensor_limits[sensor_id]

def publish_abnormal_data(topic_arn: str, sensor_data: dict, messageId: str, difference: int) -> None:
    abnormal_data = {
        **sensor_data,
        "messageId": messageId,
        "difference": difference
    }
    
    sns_client.publish_message(topic_arn, json.dumps(sensor_data))

def lambda_handler(event, context) -> dict:
    """
    Lambda handler for detecting abnormal sensor data.
    Subscribes to sns-sensors-ingress and publishes to sns-sensors-abnormal.
    """
    error_message_ids: list[str] = []
    low_topic_arn = get_env_var("SNS_ABNORMAL_LOW_TOPIC_ARN")
    high_topic_arn = get_env_var("SNS_ABNORMAL_HIGH_TOPIC_ARN")
    try:
        # Process each SNS record
        for record in event.get('Records', []):
            if record.get('EventSource') == 'aws:sns':
                messageId = None
                try:
                    message = record['Sns']['Message']
                    messageId = record['Sns']['MessageId']
                    logger.debug("Processing messageId: %s, message: %s", messageId, message)
                    
                    sensor_data = json.loads(message) # Parse the message (assuming it's JSON)
                    sensor_id, sensor_value = sensor_data.get("sensor_id"), sensor_data.get("value")
                    if sensor_id is None or sensor_value is None:
                        raise ValueError(f"Incorrect sensor data in messageId: {messageId}")
                    
                    min_value, max_value = get_sensor_limits(sensor_id)
                    logger.debug("Sensor value: %s, min_value: %s, max_value: %s", sensor_value, min_value, max_value)
                    if sensor_value < min_value:
                        publish_abnormal_data(low_topic_arn, sensor_data, messageId, min_value - sensor_value)
                        logger.debug("Sensor value is below limit: %s", sensor_value)
                    elif sensor_value > max_value:
                        publish_abnormal_data(high_topic_arn, sensor_data, messageId, max_value - sensor_value)
                        logger.debug("Sensor value is above limit: %s", sensor_value)
                    else:
                        logger.debug("Sensor value is within limits: %s", sensor_value)
                except Exception as e:
                    if messageId:
                        error_message_ids.append(messageId)
                    logger.error("Error processing sensor data: %s", e)
        return {"status": "success", "error_message_ids": error_message_ids}
    except Exception as e:
        logger.error("Error processing sensor data: %s", e)
        return {"status": "error", "error": str(e)}
