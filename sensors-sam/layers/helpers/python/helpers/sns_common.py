import boto3
from helpers.logs import get_logger
from helpers.config import get_region
logger = get_logger(__name__)

_sns_client = None

def get_sns_client(region: str = get_region()):
    global _sns_client
    if _sns_client is None:
        _sns_client = boto3.client("sns", region_name=region)
    return _sns_client

def publish_message(topic_arn, message, region: str = get_region()):
    client = get_sns_client(region)
    response =client.publish(TopicArn=topic_arn, Message=message)
    logger.debug("RESPONSE: %s", response)
    return response
