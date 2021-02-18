from mrjob.job import MRJob, MRStep
from nltk.corpus import stopwords
import re, dataset, time, nltk

WORD_RE = re.compile(r"[\w']+")

class TopTenMRJob(MRJob):

	def steps(self):
		return [
		    MRStep(mapper=self.mapper_get_words,
		           combiner=self.combiner_count_words,
		           reducer=self.reducer_count_words),
		    MRStep(reducer=self.reducer_find_top_words)
		]

	def mapper_get_words(self, _, line):
		for word in WORD_RE.findall(line):
			yield word.lower(), 1

	def combiner_count_words(self, word, counts):
		yield word, sum(counts)

	def reducer_count_words(self, word, counts):
		yield None, (sum(counts), word)

	def reducer_find_top_words(self, _, word_count_pairs):
		# Sort the key, value pairs by their counts (key) in descending order
		sorted_word_counts = sorted(list(word_count_pairs), key=lambda x : x[0], reverse=True)

		# Filter out stopwords from the list
		sorted_word_counts = list(filter(lambda x: x[1] not in stopwords.words('english'), sorted_word_counts))

		# Yield the top ten results
		yield None, sorted_word_counts[:25]

if __name__ == '__main__':
	t0 = time.time()
	TopTenMRJob.run()
	print(time.time()-t0)
