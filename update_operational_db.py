import luigi
from tweet_data_to_db import TweetDataToDb
import os


class UpdateOperationalDb(luigi.WrapperTask):

    def requires(self):
        return TweetDataToDb()
