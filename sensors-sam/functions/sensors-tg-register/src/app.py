import boto3
import cfnresponse


elbv2_client = None
def get_client():
    global elbv2_client
    if elbv2_client is None:
        elbv2_client = boto3.client("elbv2")
    return elbv2_client

def lambda_handler(event, context):
    print("EVENT: ", event)
    try:
        client = get_client()
        tg_arn = event["ResourceProperties"]["TargetGroupArn"]
        target_id = event["ResourceProperties"]["TargetId"]

        if event["RequestType"] in ("Create", "Update"):
            client.register_targets(
                TargetGroupArn=tg_arn,
                Targets=[{"Id": target_id}],
            )
        elif event["RequestType"] == "Delete":
            client.deregister_targets(
                TargetGroupArn=tg_arn,
                Targets=[{"Id": target_id}],
            )
        print("SUCCESS")
        cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
    except Exception as e:
        print("ERROR: ", e)
        cfnresponse.send(event, context, cfnresponse.FAILED, {"Error": str(e)})
