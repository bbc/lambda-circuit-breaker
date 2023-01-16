const { LambdaClient, InvokeCommand } = require("@aws-sdk/client-lambda");
const {
  SQSClient,  GetQueueUrlCommand,
  ReceiveMessageCommand, GetDeleteMessageCommand,
} = require("@aws-sdk/client-sqs");
const sqs = new SQSClient({ region: process.env.AWS_REGION });
const lambda = new LambdaClient({
    region: process.env.AWS_REGION,
    maxRetries: 0 // Avoid retries if target lambda function is timed out.
});

const queueName = process.env.QUEUE_NAME;
const functionName = process.env.FUNCTION_NAME;

exports.handler = async () => {
    console.log(`Resolve URL of queue with name ${queueName}...`);
    const urlCmd = new GetQueueUrlCommand({QueueName: queueName});
    const queueUrlResult = await sqs.send(urlCmd);
    const queueUrl = queueUrlResult.QueueUrl;

    console.log(`Poll queue ${queueUrl} for message...`);
    const rxCmd = new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        AttributeNames: ["All"]
    });
    const data = await sqs.send(rxCmd);

    if(data.Messages && data.Messages.length > 0) {
        console.log(`Message received. Invoking lambda function ${functionName} with SQS event...`);
        const sqsMessage = data.Messages[0];
        const sqsEvent = { Records: [{
            messageId: sqsMessage.MessageId,
            receiptHandle: sqsMessage.ReceiptHandle,
            body: sqsMessage.Body,
            md5OfBody: sqsMessage.MD5OfBody,
            attributes: sqsMessage.Attributes,
            messageAttributes: sqsMessage.MessageAttributes || {},
            eventSource: "aws:sqs",
            queueArn: data.QueueArn
        }]};
        console.log('sqs payload', JSON.stringify(sqsEvent));
        try {
            const command = new InvokeCommand({
                FunctionName: functionName,
                Payload: JSON.stringify(sqsEvent)
            });
            const lambdaInvokeResult = await lambda.send(command);
            console.log("Invocation result: " + JSON.stringify(lambdaInvokeResult));
            if (lambdaInvokeResult.FunctionError) {
                console.log("Invocation with function error.");
                return "failed";
            }
            console.log("Invocation succeeded.");
        } catch (e) {
            console.log("Invocation failed: " + JSON.stringify(e));
            return "failed";
       }

        console.log(`Deleting message ${sqsMessage.MessageId} from queue...`);
        const delCmd = new GetDeleteMessageCommand({
            QueueUrl: queueUrl,
            ReceiptHandle: sqsMessage.ReceiptHandle
        });
        const deleteMessageResult = await sqs.send(delCmd);
        console.log("Message deleted. Result: " + JSON.stringify(deleteMessageResult));
        return "passed";
    } else {
        console.log("No messages available.");
        return "no-message-available";
    }

}
