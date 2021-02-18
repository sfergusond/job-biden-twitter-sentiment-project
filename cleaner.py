"""
Step 2 of the project.
Once tweets have been scraped, this script will find all the parquet files in the S3 Bucket and clean them.
Initially I tried using pywren, but it wouldn't allow me to use pandas in the lambda function (see lambda_func.py)
Instead, I created my own lambda function and called it asynchronously.
Cleaned parquet files are sent as JSON files to a S3 Bucket.
"""

import boto3, time, pandas as pd

# AWS setup
s3, aws_lambda = boto3.resource('s3'), boto3.client('lambda', region_name='us-east-1')
bucket = s3.Bucket('macs30123-final')

# Find every parquet file containing scraped tweets
objects = list(bucket.objects.all())
print('Parquet files:', len(objects))
s3_paths = ['s3://macs30123-final/' + obj.key for obj in objects]

# Invoke a lambda function to clean each parquet file
for path in s3_paths:
	json = ('{"DataFrame": "%s"}' % path).encode('utf-8')
	res = aws_lambda.invoke(
		FunctionName='macs30123-cleaner',
		InvocationType='Event', # Make invocation asynchronous
		Payload=json
		)