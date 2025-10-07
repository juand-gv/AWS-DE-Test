# lambda/lf_provider/lambda_handler.py
import json
import logging
import boto3
from botocore.exceptions import ClientError

lf = boto3.client("lakeformation")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    request_type = event.get("RequestType")
    props = event.get("ResourceProperties", {})
    resource_arn = props.get("ResourceArn")
    role_arn = props.get("RoleArn")
    logger.info("LF provider event: %s %s", request_type, json.dumps(props))

    try:
        if request_type in ("Create", "Update"):
            params = {"ResourceArn": resource_arn}
            if role_arn:
                params["RoleArn"] = role_arn
                params["UseServiceLinkedRole"] = False
            else:
                params["UseServiceLinkedRole"] = True
            lf.register_resource(**params)
            logger.info("Registered LF resource %s", resource_arn)

        elif request_type == "Delete":
            try:
                lf.deregister_resource(ResourceArn=resource_arn)
                logger.info("Deregistered LF resource %s", resource_arn)
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                # Ignore NotFound-like or "must manually delete service-linked role" on delete attempt
                if code in ("EntityNotFoundException", "ResourceNotFoundException", "ValidationException") or "service-linked role" in str(e):
                    logger.warning("Ignoring delete-time LF error: %s", e)
                else:
                    raise
        return {"PhysicalResourceId": resource_arn}
    except ClientError:
        logger.exception("LF provider error")
        raise
