"""
A simple example script to get all posts on a user's timeline.
Originally created by Mitchell Stewart.
<https://gist.github.com/mylsb/10294040>
"""

import facebook
import requests


def some_action(post):
    """ Here you might want to do something with each post. E.g. grab the
    post's message (post['message']) or the post's picture (post['picture']).
    In this implementation we just print the post's created time.
    """
    print(post["created_time"])
    print(post["message"])
    print(post)


# You'll need an access token here to do anything.  You can get a temporary one
# here: https://developers.facebook.com/tools/explorer/

graph = facebook.GraphAPI(access_token)
profile = graph.get_object(user)
posts = graph.get_connections(profile["id"], "posts")
comments = graph.get_connections(id='me', connection_name='comments')

# Wrap this block in a while loop so we can keep paginating requests until
# finished.
while True:
    try:
        # Perform some action on each post in the collection we receive from
        # Facebook.
        [some_action(post=post) for post in posts["data"]]
        # Attempt to make a request to the next page of data, if it exists.
        posts = requests.get(posts["paging"]["next"]).json()
    except KeyError:
        # When there are no more pages (['paging']['next']), break from the
        # loop and end the script.
        break

#me?fields=id,name,posts{message,created_time,comments{created_time,message}}



# Local Multiprocessing (Partition by Year to each Core)
from multiprocessing import Pool
import pandas as pd

def wordCount(messages):
    year, message = messages
    return pd.DataFrame({"Word": message["Word"], "Count": message["Word"].count()}, index=[year])

with Pool(4) as p:
    results = p.map(wordCount, posts.groupby("Year"))

result_df = pd.concat(results)