import json
import oauth2 as oauth
import urllib2 as urllib

# data used to start the connection with the twitter API
all_responses = []
NUMBER_OF_TWEETS = 1000
access_token_key = "602211113-GkgphSYMEaJaBCwHEdttp4jjoaBzJkOYnlZAAAMv"
access_token_secret = "ssicR93TZkcYwzvVqNSfpwu5amaQnJej7kEPLcpt5tWyN"
consumer_key = "A3yGrn8juJV101tYiUnDg"
consumer_secret = "9VCyJMoQR7N8q75cc2ueTYDYfzjG33u3jLMeBHxVyNo"
_debug = 0
oauth_token = oauth.Token(key=access_token_key, secret=access_token_secret)
oauth_consumer = oauth.Consumer(key=consumer_key, secret=consumer_secret)
signature_method_hmac_sha1 = oauth.SignatureMethod_HMAC_SHA1()
http_method = "GET"
http_handler = urllib.HTTPHandler(debuglevel=_debug)
https_handler = urllib.HTTPSHandler(debuglevel=_debug)

def twitterreq(url, method, parameters):
    req = oauth.Request.from_consumer_and_token(oauth_consumer, token=oauth_token, http_method=http_method, http_url=url, parameters=parameters)
    req.sign_request(signature_method_hmac_sha1, oauth_consumer, oauth_token)

    if http_method == "POST":
        encoded_post_data = req.to_postdata()
    else:
        encoded_post_data = None
        url = req.to_url()

    opener = urllib.OpenerDirector()
    opener.add_handler(http_handler)
    opener.add_handler(https_handler)

    try:
        response = opener.open(url, encoded_post_data)
    except Exception as e:
        print "Exception when opening" + e

    return response

def get_twitter_data():
    tweet_samples = []
    url = "https://stream.twitter.com/1.1/statuses/sample.json"
    all_responses.append(twitterreq(url, "GET", []))

    for line in all_responses.pop():
        tweet = json.loads(line)
        if 'entities' in tweet:
            print tweet['entities']
        #if 'text' in tweet:
        #    new_tweet = [word.lower() for word in tweet['text'].split() if len(word) >= 3]
        #    tweet_samples.append(new_tweet)
        #    if len(tweet_samples) == NUMBER_OF_TWEETS:
        #        return tweet_samples;

get_twitter_data()
