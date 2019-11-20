"""
Recursive function to get all posts and comments on a user's timeline.
Originally created by Vasco Morais (November 2019).
<https://github.com/poids/FaceCloud>
"""

import requests
import re
import wordcloud

#List of Words not to include in wordcloud
STOPWORDS=wordcloud.STOPWORDS

access_token=''

response=requests.get("https://graph.facebook.com/v5.0/me?fields=id%2Cname%2Cposts%7Bmessage%2Ccreated_time%2Ccomments%7Bcreated_time%2Cmessage%7D%7D&access_token={token}".format(token=access_token))
response_json=response.json()

#Build dictionary of posts and comments
def getPostsAndComments(post, posts_and_comments):
    #Get all posts
    posts_and_comments.update(
        { post['created_time']:post['message'] }
        )
    #Get all comments
    if 'comment' in post:
        comments = post["comments"]["data"]
        posts_and_comments.update(
            { comment['created_time']:comment['message'] for comment in comments }
        )

#Recursive function which parses through all pages of facebook to return posts and comments
def requestFacebookData(response_json, posts_and_comments={}):
    if response_json['data']==[]:
        return posts_and_comments
    else:
        post_data=response_json['data']
        
        for post in post_data:
            if 'message' in post:
                getPostsAndComments(post, posts_and_comments)
        
        paging=response_json['paging']['next']
        response_json=requests.get(paging).json()
        return requestFacebookData(response_json, posts_and_comments)

#Counts all words in each posts; Groups by Year and Word
def getWordCount(posts_and_comments, fb_WordCount={}):
    for date, v in posts_and_comments.items():
        year=int(date.split('-')[0])
        vals=re.findall(r'\w+', v)
        fb_WordCount.update(
            { (year, val) : vals.count(val) for val in set(vals) if (len(val) >= 3) and val not in STOPWORDS }
            )
    return fb_WordCount

#Counts all words in each posts; Groups by Year and Word
def getWordCount(posts_and_comments, fb_WordCount={}):
    for date, v in posts_and_comments.items():
        year=int(date.split('-')[0])
        vals=re.findall(r'\w+', v)
        fb_WordCount.update(
            { (year, val) : vals.count(val) for val in set(vals) if (len(val) >= 3) and val not in STOPWORDS }
            )
    return fb_WordCount

posts_and_comments=requestFacebookData(response_json['posts'], dict())

fb_WordCount=getWordCount(posts_and_comments)








# Local Multiprocessing (Partition by Year to each Core)
from multiprocessing import Pool
import pandas as pd

def wordCount(messages):
    year, message = messages
    return pd.DataFrame({"Word": message["Word"], "Count": message["Word"].count()}, index=[year])

with Pool(4) as p:
    results = p.map(wordCount, posts_and_comments.groupby("Year"))

result_df = pd.concat(results)

#Local Multiprocessing using Dask
import dask.dataframe as dd
#Partition dataframe into the 4 Available Cores
messagesDF = dd.from_pandas(posts_and_comments, npartitions=4)
#Run parallel computations on each partition
result_df = messagesDF.groupby('Year').Word.count().compute()

#Using PySpark Locally
athlete_events_spark.groupBy('Year').count('Word').show()

#Spark Submit
spark-submit \
  --master local[4] \
  /path/to/spark-script.py

#Spark Script
if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    posts_and_comments = (spark
    .read
    .csv("/path/to/data/posts_and_comments.csv",
        header=True,
        inferSchema=True
        escape='"'))

    posts_and_comments = (posts_and_comments
    .withColumn("Words",
        posts_and_comments.Words.cast("integer")))

    print(posts_and_comments
        .groupBy('Year')
        .count('Words')
        .orderBy('Count')
        .show())
