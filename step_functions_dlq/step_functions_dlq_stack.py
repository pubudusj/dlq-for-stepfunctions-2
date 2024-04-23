from aws_cdk import (
    Duration,
    Stack,
    aws_sqs as sqs,
    CfnOutput,
    Stack,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_sqs as sqs,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_lambda_event_sources as event_sources,
)
from constructs import Construct


class StepFunctionsDlqStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        dl_queue = sqs.Queue(self, "DLQueue", queue_name="dlq-for-source-queue")

        source_queue = sqs.Queue(
            self,
            "SourceQueue",
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=1, queue=dl_queue),
            queue_name="source-queue",
        )

        set_message_visibility_timeout = tasks.CallAwsService(
            scope=self,
            id="Set message visibility timeout 5s",
            action="changeMessageVisibility",
            service="sqs",
            parameters={
                "QueueUrl.$": "$$.Execution.Input.metadata.sqs_queue_url",
                "ReceiptHandle.$": "$$.Execution.Input.metadata.sqs_receipt_handle",
                "VisibilityTimeout": 5,
            },
            iam_resources=[source_queue.queue_arn],
        )

        delete_message = tasks.CallAwsService(
            scope=self,
            id="Delete SQS Message",
            action="deleteMessage",
            service="sqs",
            parameters={
                "QueueUrl.$": "$$.Execution.Input.metadata.sqs_queue_url",
                "ReceiptHandle.$": "$$.Execution.Input.metadata.sqs_receipt_handle",
            },
            iam_resources=[source_queue.queue_arn],
        )

        simulate_message_failed_choice = (
            sfn.Choice(scope=self, id="Simulate Messages Failed?")
            .when(
                sfn.Condition.is_present("$$.Execution.Input.metadata.failed"),
                next=set_message_visibility_timeout,
            )
            .otherwise(delete_message)
        )

        message_processor_state_machine = sfn.StateMachine(
            scope=self,
            id="MessageProcessorStateMachine",
            definition_body=sfn.DefinitionBody.from_chainable(
                simulate_message_failed_choice
            ),
            timeout=Duration.minutes(2),
        )

        sf_exectuion_initializer_lambda = _lambda.Function(
            self,
            "SfExecutionInitializer",
            runtime=_lambda.Runtime.PYTHON_3_11,
            code=_lambda.Code.from_asset("lambda"),
            handler="index.handler",
            retry_attempts=0,
            environment={
                "SOURCE_QUEUE_URL": source_queue.queue_url,
                "STATE_MACHINE_ARN": message_processor_state_machine.state_machine_arn,
            },
        )
        source_queue.grant_send_messages(sf_exectuion_initializer_lambda)
        sf_exectuion_initializer_lambda.add_event_source(
            event_sources.SqsEventSource(
                source_queue, batch_size=1, report_batch_item_failures=True
            )
        )

        message_processor_state_machine.grant_start_execution(
            sf_exectuion_initializer_lambda
        )
