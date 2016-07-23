import json
import urllib2 as urllib
import oauth2 as oauth
from threading import Thread
import unicodecsv as csv
import time
import os
import twitter_logger


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
                tsvfile.writerow(["id", "created_at", "text", "favorite_count", "lang", "retweet_count", "retweeted"])
            for tweet in tweets:
                tsvfile.writerow([self.write_elem('id', tweet),
                                  self.write_elem('created_at', tweet),
                                  self.write_elem('text', tweet).encode('utf-8'),
                                  self.write_elem('favorite_count', tweet),
                                  self.write_elem('lang', tweet),
                                  self.write_elem('retweet_count', tweet),
                                  self.write_elem('retweeted', tweet)])

    def write_elem(self, key, map):
        if key in map:
            return map[key]
        else:
            return ""

