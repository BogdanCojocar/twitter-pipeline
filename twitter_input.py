import luigi
from twitter_utils import TwitterUtils


class TwitterInput(luigi.Task):

    twitter_utils = TwitterUtils()

    def run(self):
        self.twitter_utils.start_stream(seconds=10)

    def output(self):
        return luigi.LocalTarget("data/raw-tweets.tsv")

