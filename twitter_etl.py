import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs

def run_twitter_etl():
    access_key = "vZmP2wAAQ6vZpDP8OK1fmis8E"
    access_secret = "WjqKDOPGeOCliDeNqF13We0Isy6MDLD9KUUilrJ5UNjp8SyTdI"
    consumer_key = "1246236331602313221-0m6UM1Ux4zydTL6wazz2tXVpPCAbGM"
    consumer_secret = "PWwPAR9zS3fcLmyTuRPAQT0fsIdCjzxerb1BHs3vTM06y"

    #Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    #Creating an API object
    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name='@FutSheriff',
                            # 200 is the maximum allowed count
                            count=200,
                            include_rts= False,
                            # Necessary to keep full_text
                            # Otherwise only the first 140 words are extracted,
                            tweet_mode = 'extended'
                            )

    tweet_list = []

    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        tweet_list.append(refined_tweet)

    df = pd.DataFrame(tweet_list)
    df.to_csv('s3://quantaairflowbucket/futsheriff_tweets.csv')