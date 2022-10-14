from functools import partial
import time
from datetime import date
import os
import configparser
from typing import Dict, List

import boto3
import s3fs
from io import StringIO
import pandas as pd

from pipeline import Pipeline
from logger import get_log
from utils import get_data, deduplicate_data, generate_metric

LOG = get_log(__name__)
sqs = boto3.client('sqs',endpoint_url = 'https://sqs.us-east-1.amazonaws.com')
s3 = boto3.resource('s3')

pipeline = Pipeline()


@pipeline.task()
def get_raw_data(path: str) -> pd.DataFrame:
    raw_df = get_data(path)
    return raw_df

@pipeline.task(depends_on=get_raw_data)
def clean_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    dedup_df = deduplicate_data(raw_df)
    return dedup_df

@pipeline.task(depends_on=clean_data)
def get_metric(dedup_df: pd.DataFrame) -> pd.DataFrame:
    metric_df = generate_metric(dedup_df)
    return metric_df

@pipeline.task(depends_on=get_metric)
def save_metric(metric_df: pd.DataFrame) -> None:
    config = configparser.ConfigParser()
    config.read('/resources/demo.cfg')
    bucket = config['project_info']['output_bucket']
    data_key = config['project_info']['output_data_key']

    max_value_date = metric_df['date'].max()
    data_key = data_key + max_value_date + '_SearchKeywordPerformance.csv'
    output_df = metric_df[['Search_Engine_Domain','Search_Keyword','Total_Revenue']]
    csv_buffer = StringIO()
    output_df.to_csv(csv_buffer,sep='\t',index = False)
    s3.Object(bucket, data_key).put(Body=csv_buffer.getvalue())


if __name__ == "__main__":
    #Collecting queue message one by one until queue is empty
    while True:
        start = time.time()

        LOG.info("get data path from config: begin")
        config = configparser.ConfigParser()
        config.read('/resources/demo.cfg')
        queue_url = config['project_info']['input_path']

        print(queue_url)

        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=[
                'SentTimestamp'
            ],
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=0,
            WaitTimeSeconds=0
        )

        #condition to check if queue is empty
        try:
            receipt_handle = response['Messages'][0]['ReceiptHandle']
            bucket = response['Messages'][0]['Body']
            key = response['Messages'][0]['MessageAttributes']['key']['StringValue']

            path = 's3://' + bucket + '/' + key
            pipeline.run(path)

            # Delete received message from queue
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
                )


        #manage case queue is empty
        except KeyError:
            print('no messages anymore')
            break
