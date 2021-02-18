"""
Step 3 of the project.
This script assigns a sentiment score to each tweet.
Because AWS Comprehend has stringent rate limits, I can't effectively do this in parallel like the cleaning step.
Instead, each cleaned JSON file is compiled into one DataFrame using Dask.
Then, the DataFrames are split according to language (this step is needed for Comprehend).
A batch asynchronous call to the AWS Comprehend sentiment analyzer is called for each language.
After all results are returned, the text files containing the results are downloaded and parsed.
The results from the parsed files are merged back into the DataFrames, which is then concatenated into the final DataFrame
"""

import boto3, io, tarfile, json, time, pandas as pd, dask.dataframe as dd

# Constants and AWS setup
t0 = time.time()
LANGUAGES = ['de', 'en', 'es', 'it', 'pt', 'fr']
comprehend = boto3.client('comprehend', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')
print('AWS setup complete:', time.time()-t0)

# use Dask to read every clean json file from the S3 Bucket
df = dd.read_json('s3://macs30123-final-cleaned/cleaned_tweets_*.json', lines=False).compute()
print('DataFrame Shape:', df.shape)
print('Read JSON:', time.time()-t0)

# Split DataFrame by language
df_by_language = dict.fromkeys(LANGUAGES)
for language in df_by_language:
	df_by_language[language] = df[df['language'] == language]
print('Split DataFrames by language:', time.time()-t0)

# Create an AWS Comprehend Sentiment Analysis job for each language
sentiment_jobs = dict.fromkeys(LANGUAGES, {'id': None, 'status': False, 'output': None})
for language, data in df_by_language.items():

	# Encode all cleaned tweets into a file with 1 tweet per line
	file = ('\n'.join(list(data['clean_text']))).encode('utf-8')

	# Place file into S3 bucket
	s3.put_object(
		Body=file,
		Bucket='macs-30123-final-sentiment-temp',
		Key=f'tweets-{language}.txt'
		)

	# Start an async batch sentiment job
	sentiment_jobs[language]['id'] = comprehend.start_sentiment_detection_job(
		InputDataConfig={
			'S3Uri': f's3://macs-30123-final-sentiment-temp/tweets-{language}.txt',
			'InputFormat': 'ONE_DOC_PER_LINE'
			},
		OutputDataConfig={
			'S3Uri': 's3://macs-30123-final-sentiment-temp/results'
			},
		DataAccessRoleArn='arn:aws:iam::066684207108:role/comprehend-access',
		JobName=f'macs30123-sentiment-{language}',
		LanguageCode=language
		)['JobId']
print('Sent jobs to AWS Comprehend:', time.time()-t0)

# Give some time for AWS to register the jobs
time.sleep(10)

# Poll job status and continue when finished
while False in [s['status'] for s in sentiment_jobs.values()]:
	for key, job in sentiment_jobs.items():
		response = comprehend.describe_sentiment_detection_job(JobId=job['id'])['SentimentDetectionJobProperties']
		if response['JobStatus'] == 'COMPLETED':
			sentiment_jobs[key]['status'] = True
			sentiment_jobs[key]['output'] = response['OutputDataConfig']['S3Uri']

	print([s['status'] for s in sentiment_jobs.values()], end='\r')
	time.sleep(15)
print('AWS Comprehend jobs complete:', time.time()-t0)

# Download result files and merge into dataframes
for job in sentiment_jobs.values():
	key = job['output'][job['output'].find('results'):]

	# Download result
	s3.download_file(
		Bucket='macs-30123-final-sentiment-temp',
		Key=key,
		Filename=f"tmp_{job['id']}.tar.gz"
		)

	# Open results
	tar = tarfile.open(f"tmp_{job['id']}.tar.gz")
	file = tar.extractfile('output').read().decode('utf-8')

	# Parse results
	lines = file.splitlines()
	result_list = [''] * len(lines)

	# AWS Comprehend returns results out-of-order, so they must be manually placed into the correct position
	for line in lines:
		result = json.loads(line)
		result_list[result['Line']] = result['Sentiment']

	# Merge result_list into dataframe
	lang = lines[0][lines[0].find('-') + 1:lines[0].find('.txt')] # find out which language the results belong to
	df_by_language[lang]['sentiment'] = pd.Series(result_list).values
print('Parsed results:', time.time()-t0)

# Chunk dataset and upload as multiple parquet files to S3
result = pd.concat(df_by_language.values())
n_rows, chunk_size = result.shape[0], 10000
list_df = [result[i:i+chunk_size] for i in range(0, n_rows, chunk_size)]
for i, chunk in enumerate(list_df):
	file = chunk.to_json(orient='records').encode('utf-8')
	s3.put_object(Body=file, Bucket='macs-30123-final-sentiment-temp', Key=f'final/tweets_data_{i}.json', ACL='public-read')
	print(f'Uploaded {i + 1} of {len(list_df)}')