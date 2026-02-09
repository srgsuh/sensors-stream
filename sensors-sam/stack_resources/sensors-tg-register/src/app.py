import boto3
import json
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

class ELBv2Client:
    def __init__(self):
        self.client = None
    
    def get_client(self):
        if self.client is None:
            self.client = boto3.client("elbv2")
        return self.client

    def register_target(self, TargetGroupArn: str, target_id: str) -> None:
        self.get_client().register_targets(
            TargetGroupArn=TargetGroupArn,
            Targets=[{"Id": target_id}],
        )
    def deregister_target(self, TargetGroupArn: str, target_id: str) -> None:
        self.get_client().deregister_targets(
            TargetGroupArn=TargetGroupArn,
            Targets=[{"Id": target_id}],
        )

elb_client = ELBv2Client()

def lambda_handler(event, context):
    print("EVENT: ", event)
    try:
        tg_arn = event["ResourceProperties"]["TargetGroupArn"]
        target_id = event["ResourceProperties"]["TargetId"]

        if event["RequestType"] in ("Create", "Update"):
            elb_client.register_target(TargetGroupArn=tg_arn, target_id=target_id)
        elif event["RequestType"] == "Delete":
            elb_client.deregister_target(TargetGroupArn=tg_arn, target_id=target_id)
        send_response(event, context, SUCCESS, {})
    except Exception as e:
        print("ERROR: ", e)
        send_response(event, context, FAILED, {"Error": str(e)})
