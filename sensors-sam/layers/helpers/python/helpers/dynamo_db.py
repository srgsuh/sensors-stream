import os
import boto3

class DynamoDBClient:
    def __init__(self):
        self.table = None
        self.table_name = os.environ.get("DYNAMODB_TABLE_NAME")

    def get_table(self):
        if self.table is None:
            resource = boto3.resource("dynamodb")
            self.table = resource.Table(self.table_name)
        return self.table

    def put_item(self, item: dict) -> None:
        self.get_table().put_item(Item=item, ConditionExpression="attribute_not_exists(sensor_id)")

dynamodb_client = DynamoDBClient()