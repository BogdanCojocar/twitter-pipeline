import luigi
from twitter_utils import TwitterUtils
from luigi.mock import MockTarget


class TwitterInput(luigi.Task):

    twitter_utils = luigi

    def output(self):
        return MockTarget("twitter-input", mirror_on_stderr=True)

    def run(self):
        with self.output().open('w') as out:
            tweet = self.twitter_utils.get_tweet()
            print tweet
            out.write(tweet)

