import json
from helpers.logs import get_logger

logger = get_logger(__name__)

def lambda_handler(event, context):
    logger.debug("EVENT: %s", event)
    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "hello world",
            }
        ),
    }
