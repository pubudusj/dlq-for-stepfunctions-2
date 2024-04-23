import boto3
import json
import os

sqs = boto3.client("sqs")
sfn = boto3.client("stepfunctions")

SOURCE_QUEUE_URL = os.environ["SOURCE_QUEUE_URL"]
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
_SQS_MESSAGE_VISIBILITY_TIMEOUT = 300


def handler(event, context):
    sqs_message_ids = []
    for sqs_record in event["Records"]:
        try:
            message_id = sqs_record["messageId"]
        except KeyError:
            print("Error: not an SQS record")

        receipt_handle = sqs_record["receiptHandle"]
        sqs_message_ids.append(message_id)
        _set_sqs_message_visibility(SOURCE_QUEUE_URL, receipt_handle)

        _intialize_step_functions_execution(
            state_machine_arn=STATE_MACHINE_ARN,
            message_id=message_id,
            receipt_handle=receipt_handle,
            data=json.loads(sqs_record.get("body", "{}")),
        )

    return {"batchItemFailures": [{"itemIdentifier": id} for id in sqs_message_ids]}


def _set_sqs_message_visibility(queue_url: str, receipt_handle: str):
    sqs.change_message_visibility(
        QueueUrl=queue_url,
        ReceiptHandle=receipt_handle,
        VisibilityTimeout=_SQS_MESSAGE_VISIBILITY_TIMEOUT,
    )


def _intialize_step_functions_execution(
    state_machine_arn: str,
    message_id: str,
    receipt_handle: str,
    data: dict,
):
    message = {
        "metadata": {
            "message_id": message_id,
            "sqs_queue_url": SOURCE_QUEUE_URL,
            "sqs_receipt_handle": receipt_handle,
        },
        "data": data,
    }

    if data.get("failed", None):
        message["metadata"]["failed"] = True

    sfn.start_execution(
        stateMachineArn=state_machine_arn,
        input=json.dumps(message),
    )
