#Research Problem

I attempted to analyze the Twitter conversation surrounding the US election, and specifically those tweets mentioning Biden. Initially, I wanted to scrape geographic data and produce a map of showing how the sentiment of tweets are geographically arranged. Unfortunately, a vanishingly small number of tweets I scraped contained geographic data. Instead, I focus on the “who” rather than the “where”. What kind of Twitter user posts negative tweets about the election or Biden? What about positive or negative tweets?

#Steps

###Scraping
Over the course of several days, I streamed tweets pertaining to “Biden” or “#USElection2020”. Scraped tweets were sent to a Kinesis stream I had setup in my account. An AWS Firehose Stream was connected to the Kinesis stream, which took each record, processed it into parquet format, and uploaded a parquet file to an S3 Bucket every 900 seconds. In total I scraped about 300,000 tweets. My Twitter API limit was 500,000 tweets, so I left room to scrape more if anything happened to the original dataset. For each tweet, I downloaded the id, text, follower count of the user, and the user’s description. Code in `twitter.py`.

###Cleaning	
Next, I cleaned the text/description and assigned a language for each entry. Initially I tried using pywren, but because I needed pandas, pywren would not work. Instead, I made my own Lambda function by zipping up the dependencies in a layer and deploying the code found in `lambda_func.py`. For each parquet file containing the raw twitter data, I asynchronously invoked my lambda function. Cleaning all the tweets took about 10-15s including the invocation overhead, so it functioned exactly like pywren. Cleaned parquet files were uploaded to a second S3 Bucket in json format. Code in `cleaner.py`.

###Sentiment Scoring
Next, I needed to figure out the sentiment of each tweet. To do so, I used AWS Comprehend’s sentiment analyzer. Given the rate limits of AWS Comprehend, the best strategy was a serial one. I gathered all the cleaned twitter text into one text file per language (one tweet per line) and uploaded them to an S3 Bucket. Then, I called the AWS Comprehend analyzer on each uploaded document. This worked much faster than the parallel alternative, as I would have needed to chunk the data into 25 tweets, then call a maximum of 10 concurrent Comprehend tasks at a time. The code waits for the sentiment analyzer to finish, then downloads and unzips the results files. The unzipped results were in json format, and unfortunately out of order. Therefore, I needed to serially place each result line into the correct spot in the DataFrame. Finally, I chunked the DataFrame with the sentiment scores and uploaded them to an S3 Bucket as json files. Code in `sentiment.py`.

###Analysis	
Finally, I analyzed the dataset in two ways. First, I created a couple simple machine learning models using PySpark on an AWS EMR cluster. One model looked at the relationship between sentiment and follower counts, and the other model looked at user description words frequencies and sentiment. I also used mrjob to find the top 25 words in user descriptions for each sentiment score (positive, negative, neutral). Code in `ML-Notebook.ipynb` and `topWords.py`.

#Results

The machine learning model using followers count as the feature had a low overall test accuracy of 34.4%. However, breaking down the predictions by label showed an interesting relationship. Based on the FPR/TPR reports, the model struggled to differentiate between positive and negative sentiments. However, it was more confident with neutral sentiments. Grouping the data by sentiment score and averaging the follower counts resulted in relatively equivalent follower counts between the positive and negative labels, but vastly higher (four times as many) follower counts for the neutral label. This likely means that high-profile accounts, such as politicians and news outlets, produce neutral-toned tweets on the election while accounts belonging to private individuals charge their posts with more emotions. 

The model using word frequencies of user descriptions was very uncertain, which I found surprising. The model resulted in equal counts of inaccurate and accurate predictions, meaning that no combination of words in a user’s description could accurately predict how they felt about Biden or the US Election. My initial hypothesis is that Democrat-leaning posters (presumably with positive things to say) would have different descriptions than Republican-leaning posters (presumably with negative things to say). 
	
Producing the top 25 words of description by each label using mrjob shed light on this confusing outcome:

POSITIVE: [[2424, "trump"], [1766, "love"], [1523, "maga"], [1033, "proud"], [1015, "god"], [987, "life"], [846, "lover"], [799, "mom"], [792, "american"], [766, "conservative"], [761, "patriot"], [728, "president"], [676, "retired"], [674, "country"], [672, "wife"], [632, "family"], [623, "fan"], [618, "resist"], [610, "truth"], [602, "america"], [578, "kag"], [564, "i'm"], [552, "father"], [548, "husband"], [525, "biden"]]

NEGATIVE: [[13609, "trump"], [9335, "maga"], [8381, "love"], [5296, "god"], [4851, "conservative"], [4596, "life"], [4549, "proud"], [4327, "patriot"], [4260, "american"], [3597, "country"], [3493, "america"], [3456, "president"], [3383, "kag"], [3378, "retired"], [3192, "family"], [3149, "mom"], [3131, "lover"], [2999, "truth"], [2927, "christian"], [2838, "i'm"], [2837, "fan"], [2833, "wife"], [2699, "pro"], [2652, "father"], [2631, "people"]]

NEUTRAL: [[16518, "trump"], [12316, "maga"], [10071, "news"], [9972, "love"], [6755, "god"], [6527, "conservative"], [6045, "life"], [5656, "american"], [5549, "proud"], [5528, "patriot"], [4960, "america"], [4655, "retired"], [4599, "kag"], [4517, "country"], [4491, "world"], [4331, "us"], [4274, "truth"], [4269, "president"], [4187, "politics"], [4181, "i'm"], [4108, "follow"], [4036, "family"], [3957, "christian"], [3895, "fan"], [3721, "mom"]]

Across all three sentiments, the top word is “trump”, implying that the most common user archetype was a Trump supporter. Farther down the positive list we do see “biden” and “resist”, suggesting the presence of left-leaning posters. In the negative list we see “world”, “new”, and “politics”, suggesting the presence of news outlets in that category. By and large, terms like “christian”, “god”, “mom”, “family”, “wife”, “retired”, “kag”, “maga”, “conservative” are on each of the lists, denoting a massive older and rightward skew of the Twitter posters. 

So, the “who” of people talking about the election on Twitter are resoundingly older and conservative. An interesting finding given the conservative outcry over platforms like Twitter “censoring” their content.
	
	
