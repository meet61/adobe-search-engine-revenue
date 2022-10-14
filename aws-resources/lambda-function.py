import boto3

s3 = boto3.resource('s3')
sqs = boto3.client('sqs')
stepFunction = boto3.client('stepfunctions')
queue_url = 'https://sqs.us-east-1.amazonaws.com/487388507517/wesite-hit-files-queue'

def lambda_handler(s3_event, context):
    # Get the s3 event
    for record in s3_event.get("Records"):
        bucket = record.get("s3").get("bucket").get("name")
        key = record.get("s3").get("object").get("key")

        # Send message to SQS queue
        print(bucket,key)
        response = sqs.send_message(
            QueueUrl=queue_url,
            DelaySeconds=1,
            MessageAttributes={
                'key': {
                    'DataType': 'String',
                    'StringValue': key
                }
            },
            MessageBody=(bucket)
        )
        #RUN stepfunction
        response = stepFunction.start_execution(
            stateMachineArn='arn:aws:states:us-east-1:487388507517:stateMachine:Step-search-engine-revenue'
    )
