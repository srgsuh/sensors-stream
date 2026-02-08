import json
from helpers.logs import get_logger
from helpers.sns_common import publish_message
from helpers.config import (
    ConfigurationError, InternalServerError, get_env_var
)

APP_PATH = "/api/v1/sensors"

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

ok_response: dict = build_response(200, "Accepted")

def get_request_body(event: dict) -> dict:
    return json.loads(event["body"])

def lambda_handler(event, context):
    logger.debug("EVENT: %s", event)
    response = ok_response
    try:
        path = event["path"]
        method = event["httpMethod"]
        logger.debug("PATH: %s, METHOD: %s", path, method)
        if path != APP_PATH or method != "POST":
            logger.warning("Invalid path or method: %s, %s", path, method)
            response = build_response(404, "Not Found")
        else:
            request_body = get_request_body(event)
            logger.debug("REQUEST BODY: %s", request_body)
            topic_arn = get_env_var("SNS_TOPIC_ARN")
            logger.debug("TOPIC ARN: %s", topic_arn)
            topic_response = publish_message(topic_arn, json.dumps(request_body))
            logger.debug("TOPIC RESPONSE: ", topic_response)
    except (ConfigurationError, InternalServerError) as e:
        logger.error("Internal Server Error: %s", e)
        response = build_response(500, "Internal Server Error")
    except Exception as e:
        logger.error("Error parsing request body: %s", e)
        response = build_response(400, "Bad Request")

    logger.debug("RESPONSE: ", response)
    return response