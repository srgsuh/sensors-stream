import os
from typing import Optional
import boto3

_dynamodb_resource = None

def _get_dynamodb_resource():
    global _dynamodb_resource
    if _dynamodb_resource is None:
        _dynamodb_resource = boto3.resource("dynamodb")
    return _dynamodb_resource

_dynamodb_tables = {}

def get_dynamodb_table(table_name: str):
    global _dynamodb_tables
    if table_name not in _dynamodb_tables:
        _dynamodb_tables[table_name] = _get_dynamodb_resource().Table(table_name)
    return _dynamodb_tables[table_name]

class DynamoDBTableClient:
    def __init__(self, table_name: str):
        self.table = None
        self.table_name = table_name

    def get_table(self):
        if self.table is None:
            self.table = get_dynamodb_table(self.table_name)
        return self.table
    
    def get_item(self, sensor_id: str) -> Optional[dict]:
        response = self.get_table().get_item(Key={"sensor_id": sensor_id})
        return response.get("Item")

    def delete_item(self, sensor_id: str) -> None:
        self.get_table().delete_item(
            Key={"sensor_id": sensor_id}
        )

    def put_item(self, item: dict) -> None:
        self.get_table().put_item(Item=item, ConditionExpression="attribute_not_exists(sensor_id)")


parameters_table_client = DynamoDBTableClient(table_name="sensor-parameters")

def get_sensor_parameters(sensor_id: str) -> Optional[dict]:
    return parameters_table_client.get_item(sensor_id)

def get_all_sensor_parameters() -> list[dict]:
    table = parameters_table_client.get_table()
    response = table.scan()
    return response.get("Items", [])