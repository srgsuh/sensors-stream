import json
import uuid
from helpers.logs import get_logger
from helpers.sns_common import sns_client
from helpers.config import InternalServerError, get_env_var

class UnsupportedEndpointError(Exception):
    pass

class InvalidRequestError(Exception):
    pass

logger = get_logger(__name__)

def build_response(status_code: int, message: str) -> dict:
    return {
        "statusCode": status_code,
        "body": json.dumps(
            {
                "message": message,
            }
        ),
    }

def get_path_and_method(event: dict) -> tuple[str, str]:
    path, method = event.get("path", ""), event.get("httpMethod", "")
    logger.debug("PATH: %s, METHOD: %s", path, method)
    return path, method

def validate_path_and_method(path: str, method: str, app_path: str, methods: list[str] = ["POST"]) -> None:
    if path != app_path or method not in methods:
        logger.error("Invalid path or method: %s, %s", path, method)
        raise UnsupportedEndpointError("Invalid path or method")

def validate_request_body(body: dict) -> None:
    if "sensor_id" not in body:
        raise InvalidRequestError("Sensor ID is required")
    if "value" not in body:
        raise InvalidRequestError("Value is required")

def get_request_body(event: dict) -> dict:
    try:
        body: dict = json.loads(event["body"])
        logger.debug("REQUEST BODY: %s", body)

        validate_request_body(body)

        return body
    except ValueError as e:
        logger.error(f"Error parsing request body: {e}")
        raise InvalidRequestError(f"Error parsing request body: {e}")

def build_sns_message(body: dict) -> dict:
    package_id = str(uuid.uuid4())
    logger.debug("PACKAGE ID: %s", package_id)
    return {
        "package_id": package_id,
        **body,
    }

def publish_sns_message(message: dict) -> None:
    try:
        topic_arn = get_env_var("SNS_TOPIC_ARN")
        logger.debug("TOPIC ARN: %s", topic_arn)
        topic_response = sns_client.publish_message(topic_arn, json.dumps(message))
        logger.debug("TOPIC RESPONSE: %s", topic_response)
        return topic_response
    except Exception as e:
        logger.error(f"Error publishing SNS message: {e}")
        raise InternalServerError(f"Error publishing SNS message: {e}")