import json
import urllib2 as urllib
import oauth2 as oauth
from threading import Thread
import unicodecsv as csv


class TwitterUtils:

    number_of_tweets = 100
    http_method = "GET"
    tweets = []

    def __init__(self):
        Thread(target=self.__start_tweeter_stream).start()

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
            try:
                tweet = json.loads(line)
            except ValueError:
                continue

            self.tweets.append(tweet)

            if len(self.tweets) == self.number_of_tweets:
                self.write_tweets(self.tweets)
                del self.tweets[:]

    def write_tweets(self, tweets):
        with open('test.csv', 'wb+') as tsvout:
            tsvfile = csv.writer(tsvout, delimiter="\t", encoding='utf-8', lineterminator='\n')

            # write header
            tsvfile.writerow(["text", "favorite_count"])
            for tweet in tweets:
                tsvfile.writerow([self.write_elem('text', tweet),
                                  self.write_elem('favorite_count', tweet)])

    def write_elem(self, key, map):
        if key in map:
            return map[key]
        else:
            return "NULL"


if __name__ == '__main__':
    tu = TwitterUtils()


