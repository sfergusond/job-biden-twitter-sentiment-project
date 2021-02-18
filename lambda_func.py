"""
My bespoke lambda function used to clean each scraped tweet in parallel.
First, it cleans the text and user description field, then it assigns a langauge to each tweet
"""

import re, random, string, pandas as pd, awswrangler as awr
from bs4 import BeautifulSoup
from langdetect import detect

def clean_all_text(file):
	"""
	Params
	------
	file : str
	An AWS S3 key pertaining to a parquet file

	Returns
	-------
	df : Pandas DataFrame
	A DataFrame with the cleaned text and sentiment score

	Cleans the text (tweet text, user description) in each row of the parquet file, then finds and assigns a sentiment for each row
	"""

	def detect_language(text):
		try:
			return detect(text)
		except:
			return 'NA'

	def clean_text(text):
		"""
		Returns a cleaned version of the raw Twitter text
		"""

		# Remove URLs
		cleaned = re.sub('https?://[A-Za-z0-9./]+', '', text)
		# Decode HTML
		decode_html = BeautifulSoup(cleaned, 'lxml')
		cleaned = decode_html.get_text()
		# Remove mentions
		cleaned = re.sub(r'@[A-Za-z0-9_]+', '', cleaned)
		# Remove unicode
		cleaned = re.sub(r'[^\x00-\x7F]+','', cleaned)
		# Remove numbers and hashtags
		cleaned = re.sub("[^a-zA-Z']", ' ', cleaned)
		# Strip leading/ending whitespace
		cleaned = cleaned.strip()
		# Make all lowercase
		cleaned = cleaned.lower()

		return cleaned

	# Read parquet file into DataFrame
	df = awr.s3.read_parquet(file)

	# Clean Tweet text and description
	df['clean_text'] = df['text'].map(clean_text)
	df['clean_description'] = df['description'].map(clean_text)

	# Detect language of tweet
	df['language'] = df['text'].map(detect_language)

	# Keep only Latin-script langauges supported by AWS Comprehend
	lang_error = df['language'].isin(['de', 'en', 'es', 'it', 'pt', 'fr'])
	df = df[lang_error]

	# Drop rows with missing values, or text fields without significant content
	df = df.dropna()
	df = df[df.clean_text.apply(lambda x: len(x) > 5)]

	# Send cleaned DataFrame as a json file to S3
	salt = ''.join(random.choices(string.ascii_uppercase + string.digits, k=15))
	path =  f's3://macs30123-final-cleaned/cleaned_tweets_{salt}.json'
	awr.s3.to_json(df, path, orient='records')

	return path

def handler(event, context):
	return {'Path': clean_all_text(event['DataFrame'])}