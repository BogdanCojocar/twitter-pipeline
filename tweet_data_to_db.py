import luigi.postgres
from datetime import date
from twitter_input import TwitterInput


class TweetDataToDb(luigi.postgres.CopyToTable):

    date = luigi.DateParameter(default=date.today())

    host = "localhost"
    database = "twitter"
    user = "postgres"
    password = ""
    table = "tweet"

    columns = []

    def requires(self):
        return TwitterInput(self.date)