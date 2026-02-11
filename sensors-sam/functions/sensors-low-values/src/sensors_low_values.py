import json
from helpers.logs import get_logger

logger = get_logger("abnormal-low")

def log_low_value(message: str) -> None:
    try:
        payload = json.loads(message)
    except json.JSONDecodeError:
        logger.warning("Skipping non-JSON SNS message: %s", message)
        return

    sensor_id = payload.get("sensor_id")
    if sensor_id is None:
        logger.warning("Skipping message without sensor ID: %s", message)
        return
    package_id = payload.get("package_id", "undefined")
    sensor_value = payload.get("value", "undefined")
    deviation = payload.get("deviation", "undefined")
    logger.info(
        "PackageID: %s. Sensor %s abnormal LOW value = %s with deviation = %s",
        package_id, sensor_id, sensor_value, deviation
    )

def lambda_handler(event, context):
    records = event.get("Records", [])
    for record in records:
        message = record.get("Sns", {}).get("Message", "")
        log_low_value(message)
    return {"statusCode": 200}
