import boto3
from botocore.exceptions import ClientError
from helpers.logs import get_logger
from helpers.config import get_region, InternalServerError


logger = get_logger(__name__)

_sns_client = None

def get_sns_client(region: str | None = None):
    region = region or get_region()
    global _sns_client
    if _sns_client is None:
        _sns_client = boto3.client("sns", region_name=region)
    return _sns_client

def publish_message(topic_arn, message, region: str | None = None):
    try:
        client = get_sns_client(region)
        response = client.publish(TopicArn=topic_arn, Message=message)
        logger.debug("RESPONSE: %s", response)
        return response
    except ClientError as e:
        logger.error("Error publishing message to SNS: %s", e)
        raise InternalServerError(f"Error publishing message to SNS: {e}")
    except Exception as e:
        logger.error("Error publishing message to SNS: %s", e)
        raise InternalServerError(f"Error publishing message to SNS: {e}")
