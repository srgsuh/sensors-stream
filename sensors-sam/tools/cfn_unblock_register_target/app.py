import json
import logging
import urllib.request

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


SUCCESS = "SUCCESS"
FAILED = "FAILED"


def cfn_send(
    event,
    context,
    response_status,
    response_data,
    physical_resource_id=None,
    no_echo=False,
    reason=None,
):
    """
    Minimal CloudFormation custom resource response helper.
    Avoids relying on the 'cfnresponse' module which is not present by default.
    """
    response_url = event["ResponseURL"]

    body = {
        "Status": response_status,
        "Reason": reason
        or f"See details in CloudWatch Log Stream: {getattr(context, 'log_stream_name', 'unknown')}",
        "PhysicalResourceId": physical_resource_id
        or getattr(context, "log_stream_name", "unknown"),
        "StackId": event["StackId"],
        "RequestId": event["RequestId"],
        "LogicalResourceId": event["LogicalResourceId"],
        "NoEcho": no_echo,
        "Data": response_data or {},
    }

    json_body = json.dumps(body).encode("utf-8")
    headers = {"content-type": "", "content-length": str(len(json_body))}

    req = urllib.request.Request(
        response_url,
        data=json_body,
        headers=headers,
        method="PUT",
    )
    with urllib.request.urlopen(req) as resp:
        logger.info("Sent CFN response. status_code=%s", resp.status)


_elbv2 = None


def _client():
    global _elbv2
    if _elbv2 is None:
        _elbv2 = boto3.client("elbv2")
    return _elbv2


def lambda_handler(event, context):
    logger.info("EVENT: %s", json.dumps(event))

    try:
        tg_arn = event["ResourceProperties"]["TargetGroupArn"]
        target_id = event["ResourceProperties"]["TargetId"]
        request_type = event.get("RequestType")

        client = _client()

        if request_type in ("Create", "Update"):
            client.register_targets(
                TargetGroupArn=tg_arn,
                Targets=[{"Id": target_id}],
            )
        elif request_type == "Delete":
            # Best-effort deregistration; deletion should complete even if already gone.
            try:
                client.deregister_targets(
                    TargetGroupArn=tg_arn,
                    Targets=[{"Id": target_id}],
                )
            except Exception as e:
                logger.warning("Deregister failed (ignored): %s", e)

        cfn_send(event, context, SUCCESS, {})
        return {"ok": True}
    except Exception as e:
        logger.exception("Handler failed")
        try:
            cfn_send(event, context, FAILED, {"Error": str(e)}, reason=str(e))
        except Exception:
            # If we can't even send CFN response, nothing else to do.
            pass
        raise

