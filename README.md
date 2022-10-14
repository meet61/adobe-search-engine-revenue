
#  How to Execute code

1. Move to the root of project.

2. Create Docker Image:
    ```
    docker build -t dev-mpatel073 .
    ```
3. Tag the docker image with appropriate AWS account
    ```
    docker tag dev-mpatel073:latest XXXXXXXXXXX.dkr.ecr.us-east-1.amazonaws.com/dev-mpatel073:etl-search-engine-rev
    ```
4. Push the docker image to ECR
    ```
    docker push XXXXXXXXXX.dkr.ecr.us-east-1.amazonaws.com/dev-mpatel073:etl-search-engine-rev
    ```
5. Upload the source file to S3 bucket
   ```
   aws s3 sync RevData/ s3://website-hit-data/data/
   ```
6. Above steps will trigger lambda functions. Lambda function will trigger workflow. Upon completion output file will be created in s3://website-hit-data/  processed/ bucket




#  AWS Workflow

![alt text](https://github.com/meet61/adobe-search-engine-revenue/blob/main/WorkflowImage.png?raw=true)


1. Create a S3 bucket.

2. Create a queue SQS.

3. Code your lambda function triggered on S3 event to send a message to queue.

4. Create your scripts for preprocessing your data.

5. Build a Docker Image to run your scripts.

6. Push your Docker Image to ECR.

7. Create Fargate Task and Cluster.

8. Orchestrate your pipeline with Step-function.
