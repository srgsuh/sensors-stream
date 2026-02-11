import json
from helpers.logs import get_logger
from helpers.config import get_env_var
from helpers.sns_common import sns_client
from helpers.dynamo_db import get_sensor_parameters

logger = get_logger("sensors-abnormal")

_sensor_limits: dict[str, tuple[int, int]] = {}

def get_low_topic_arn() -> str:
    return get_env_var("SNS_ABNORMAL_LOW_TOPIC_ARN")

def get_high_topic_arn() -> str:
    return get_env_var("SNS_ABNORMAL_HIGH_TOPIC_ARN")

def get_sensor_limits(sensor_id: str) -> tuple[int, int]:
    if sensor_id not in _sensor_limits:
        params = get_sensor_parameters(sensor_id)
        if not params:
            raise ValueError(f"Sensor bounds not found for sensor_id: {sensor_id}")
        _sensor_limits[sensor_id] = (params["min_value"], params["max_value"])
    return _sensor_limits[sensor_id]

def publish_abnormal_data(topic_arn: str, sensor_data: dict, deviation: int) -> None:
    abnormal_data = {
        **sensor_data,
        "deviation": deviation
    }
    sns_client.publish_message(topic_arn, json.dumps(abnormal_data))

def process_record(record: dict) -> None:
    message = record.get("body")
    logger.debug("Processing message: %s", message)
    
    sensor_data = json.loads(message or "") # Parse the message (assuming it's JSON)
    package_id = sensor_data.get("package_id")
    logger.debug("Package ID: %s", package_id)
    sensor_id, sensor_value = sensor_data.get("sensor_id"), sensor_data.get("value")
    if sensor_id is None or sensor_value is None:
        raise ValueError(f"Incorrect sensor data in package: {package_id}")
    
    min_value, max_value = get_sensor_limits(sensor_id)
    logger.debug("Sensor value: %s, min_value: %s, max_value: %s", sensor_value, min_value, max_value)
    if sensor_value < min_value:
        publish_abnormal_data(get_low_topic_arn(), sensor_data, sensor_value - min_value)
        logger.debug("Sensor %s value %s is below limit: %s", sensor_id, sensor_value, min_value)
    elif sensor_value > max_value:
        publish_abnormal_data(get_high_topic_arn(), sensor_data, sensor_value - max_value)
        logger.debug("Sensor %s value %s is above limit: %s", sensor_id, sensor_value, max_value)
    else:
        logger.debug("Sensor %s value %s is within limits: %s", sensor_id, sensor_value, min_value, max_value)

def lambda_handler(event, context) -> dict:
    """
    Lambda handler for detecting abnormal sensor data.
    Subscribes to sns-sensors-ingress and publishes to sns-sensors-abnormal.
    """
    try:
        logger.debug("EVENT: %s", event)
        records = event.get("Records", [])
        logger.debug("%d records received", len(records))
        batch_item_failures = []
        for record in records:
            messageId = record.get("messageId")
            logger.debug("Message ID: %s", messageId)
            try:
                process_record(record)
            except Exception as e:
                batch_item_failures.append({"itemIdentifier": messageId})
                logger.error("Error processing message %s: %s", messageId, e)
        logger.info("%d messages processed successfully, %d messages failed", len(records) - len(batch_item_failures), len(batch_item_failures))
        return {"batchItemFailures": batch_item_failures}
    except Exception as e:
        logger.error("Error processing event: %s", e)
        return {}
