import os
import json
import boto3
import urllib.request

SUCCESS = "SUCCESS"
FAILED = "FAILED"

def send_response(event, context, status, data=None):
    response_body = {
        "Status": status,
        "Reason": f"See logs: {context.log_stream_name}",
        "PhysicalResourceId": context.log_stream_name,
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "Data": data or {},
    }

    req = urllib.request.Request(
        event["ResponseURL"],
        data=json.dumps(response_body).encode("utf-8"),
        headers={"Content-Type": ""},
        method="PUT",
    )
    urllib.request.urlopen(req)

def seed_default_parameters() -> None:
    """Seed the DynamoDB table with default sensor parameters on stack creation."""
    default_params = [
        {
            "sensor_id": "101",
            "min_value": 43,
            "max_value": 73,
        },
        {
            "sensor_id": "102",
            "min_value": 52,
            "max_value": 82,
        },
        {
            "sensor_id": "103",
            "min_value": 38,
            "max_value": 68,
        },
        {
            "sensor_id": "104",
            "min_value": 55,
            "max_value": 85,
        },
        {
            "sensor_id": "105",
            "min_value": 25,
            "max_value": 35,
        },
        {
            "sensor_id": "106",
            "min_value": 56,
            "max_value": 86,
        },
        {
            "sensor_id": "107",
            "min_value": 17,
            "max_value": 47,
        },
        {
            "sensor_id": "108",
            "min_value": 68,
            "max_value": 98,
        },
    ]
    table_name = os.environ.get("DYNAMODB_TABLE_NAME")
    resource = boto3.resource("dynamodb")
    table = resource.Table(table_name)

    for item in default_params:
        table.put_item(Item=item, ConditionExpression="attribute_not_exists(sensor_id)")
    


def lambda_handler(event, context):
    try:
        print("EVENT: ", json.dumps(event))
        request_type = event["RequestType"]

        if request_type == "Create":
            result = seed_default_parameters()
            send_response(event, context, SUCCESS, result)
        elif request_type == "Update":
            result = seed_default_parameters()
            send_response(event, context, SUCCESS, result)
        elif request_type == "Delete":
            # Nothing to clean up — table deletion is handled by CloudFormation
            send_response(event, context, SUCCESS, {})
        else:
            send_response(event, context, FAILED, {"Error": f"Unknown request type: {request_type}"})
    except Exception as e:
        print("ERROR: ", e)
        send_response(event, context, FAILED, {"Error": str(e)})
