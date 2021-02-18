"""
Step 1 of the project. 
The code below simply scrapes real-time tweets pertaining to 'Biden' or the hashtag '#USElection2020'
Scraped tweets are sent to a Kinesis stream I have setup in my account.
An AWS Firehose Stream is connected to the 'macs30123-final' stream, which takes each record, processes it into parquet format, and uploads a parquet file
to an S3 Bucket every 900 seconds.
"""

import sys, boto3, json
from tweepy import OAuthHandler, API, Stream
from tweepy.streaming import StreamListener

# Twitter API/AWS setup
kinesis = boto3.client('kinesis', region_name='us-west-1')
auth = OAuthHandler('****', '****')
auth.set_access_token('****', '****')
api = API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

class Listener(StreamListener):

	def __init__(self):
		super(Listener,self).__init__()
		self.count = 0

	def on_status(self, status):
		# Ensure tweets have text, a user with a description, and aren't retweets or plain links
		if status.text and status.user.description and status.text[:2] != 'RT' and status.text[:3] != 'http':
			# Bundle the tweet data into json format
			data = {'id': status.id, 'text': status.text, 'description': status.user.description, 'followers': status.user.followers_count}
			data = json.dumps(data)

			# Update the count and print progress to console
			self.count += 1
			print(self.count, end='\r')

			# Send the json to the kinesis stream
			kinesis.put_record(
				StreamName='macs30123-final',
				Data=data,
				PartitionKey='partitionkey'
				)

	def on_error(self, status_code):
		print(status_code)
		return False

stream = Stream(auth=api.auth, listener=Listener())

try:
    print('Start streaming.')
    stream.filter(track=['Biden, #USElection2020'])
except KeyboardInterrupt:
    print("Stopped.")
finally:
    print('Done.')
    stream.disconnect()