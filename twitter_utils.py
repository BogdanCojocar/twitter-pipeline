import json
import urllib2 as urllib
import oauth2 as oauth
from threading import Thread
import unicodecsv as csv
import time
import os
import twitter_logger
from string_constants import *


class TwitterUtils:

    log = twitter_logger.get_logger(__name__)

    number_of_tweets = 100
    http_method = "GET"
    tweets = []
    path = "data/raw-tweets.tsv"
    stop_stream = False

    def start_stream(self, seconds):
        self.log.info("twitter stream started for %d seconds" % seconds)

        Thread(target=self.__start_tweeter_stream).start()
        time.sleep(seconds)

        self.stop_stream = True
        self.log.debug("twitter stream stoped")

    def __twitterreq(self, url, method, parameters):
        oauth_token = oauth.Token(key="602211113-GkgphSYMEaJaBCwHEdttp4jjoaBzJkOYnlZAAAMv",
                                  secret="ssicR93TZkcYwzvVqNSfpwu5amaQnJej7kEPLcpt5tWyN")
        oauth_consumer = oauth.Consumer(key="A3yGrn8juJV101tYiUnDg",
                                        secret="9VCyJMoQR7N8q75cc2ueTYDYfzjG33u3jLMeBHxVyNo")
        req = oauth.Request.from_consumer_and_token(consumer=oauth_consumer,
                                                    token=oauth_token,
                                                    http_method=self.http_method,
                                                    http_url=url,
                                                    parameters=parameters)

        req.sign_request(oauth.SignatureMethod_HMAC_SHA1(),
                         oauth_consumer,
                         oauth_token)

        if self.http_method == "POST":
            encoded_post_data = req.to_postdata()
        else:
            encoded_post_data = None
            url = req.to_url()

        opener = urllib.OpenerDirector()
        _debug = 0
        opener.add_handler(urllib.HTTPHandler(debuglevel=_debug))
        opener.add_handler(urllib.HTTPSHandler(debuglevel=_debug))

        try:
            response = opener.open(url, encoded_post_data)
        except Exception as e:
            print "Exception when opening" + e

        return response

    def __start_tweeter_stream(self):

        for line in self.__twitterreq(url="https://stream.twitter.com/1.1/statuses/sample.json",
                                      method=self.http_method,
                                      parameters=[]):

            if self.stop_stream:
                return

            try:
                tweet = json.loads(line)
            except ValueError:
                continue

            # ignore deleted data
            if 'id' in tweet:
                self.tweets.append(tweet)

            if len(self.tweets) == self.number_of_tweets:
                self.write_tweets(self.tweets)
                del self.tweets[:]

    def write_tweets(self, tweets):

        self.log.debug("writing tweets in file")
        mode = 'a' if os.path.exists(self.path) else 'w'
        with open(self.path, mode) as tsvout:
            tsvfile = csv.writer(tsvout, delimiter="\t", encoding='utf-8', lineterminator='\n')

            if mode == 'w':
                # write header
                tsvfile.writerow([ID, CREATED_AT, TEXT, FAVORITE_COUNT, LANG, RETWEET_COUNT, RETWEETED,
                                  COUNTRY, COUNTRY_CODE, PLACE_NAME, USER_ID, USER_DESCRIPTION,
                                  USER_FAVORITE_COUNT, USER_FOLLOWING, USER_FOLLOWERS_COUNT, USER_LOCATION,
                                  HASHTAGS])

            for tweet in tweets:
                tsvfile.writerow([self.write_elem(ID, tweet),
                                  self.write_elem(CREATED_AT, tweet),
                                  self.write_elem(TEXT, tweet),
                                  self.write_elem(FAVORITE_COUNT, tweet),
                                  self.write_elem(LANG, tweet),
                                  self.write_elem(RETWEET_COUNT, tweet),
                                  self.write_elem(RETWEETED, tweet),
                                  self.write_elem(PLACE, tweet, COUNTRY),
                                  self.write_elem(PLACE, tweet, COUNTRY_CODE),
                                  self.write_elem(PLACE, tweet, NAME),
                                  self.write_elem(USER, tweet, ID),
                                  self.write_elem(USER, tweet, DESCRIPTION),
                                  self.write_elem(USER, tweet, FAVORITE_COUNT),
                                  self.write_elem(USER, tweet, FOLLOWING),
                                  self.write_elem(USER, tweet, FOLLOWERS_COUNT),
                                  self.write_elem(USER, tweet, LOCATION),
                                  self.write_elem(ENTITIES, tweet, HASHTAGS)])

    # TODO: find a recursive way to do this
    # I'm not proud of this method
    def write_elem(self, key, map, second_key='NULL'):

        if key in map and map[key] is not None:
            if second_key != NULL:
                if second_key in map[key] and map[key][second_key] is not None:
                    if second_key == HASHTAGS:
                        hashtags = map[key][second_key]
                        print hashtags
                    return map[key][second_key]
            else:
                return map[key]

        if second_key in [FAVORITE_COUNT, FOLLOWERS_COUNT]:
            return 0
        elif second_key in [FOLLOWING]:
            return False
        else:
            return NULL

