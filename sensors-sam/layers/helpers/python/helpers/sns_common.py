import boto3
from botocore.exceptions import ClientError
from helpers.logs import get_logger
from helpers.config import get_region, InternalServerError

logger = get_logger(__name__)

class SNSClient:
    def __init__(self):
        self.clients: dict = {}
    
    def get_client(self, region: str | None = None):
        region = region or get_region()
        if region not in self.clients:
            self.clients[region] = boto3.client("sns", region_name=region)
        return self.clients[region]

    def publish_message(self, topic_arn: str, message: str, region: str | None = None):
        try:
            client = self.get_client(region)
            response = client.publish(TopicArn=topic_arn, Message=message)
            logger.debug("RESPONSE: %s", response)
            return response
        except ClientError as e:
            logger.error("Error publishing message to SNS: %s", e)
            raise InternalServerError(f"Error publishing message to SNS: {e}")
        except Exception as e:
            logger.error("Unexpected error of SNS client: %s", e)
            raise InternalServerError(f"Error publishing message to SNS: {e}")


sns_client = SNSClient()
