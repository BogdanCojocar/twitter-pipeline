from twitter_input import TwitterInput
import luigi
from luigi import postgres
import time


class TweetDataToDb(luigi.postgres.CopyToTable):

    host = "localhost"
    database = "twitter"
    user = "postgres"
    password = ""
    table = "tweet"

    columns = [("id", "BIGINT"),
               ("created_at", "TIMESTAMP WITHOUT TIME ZONE"),
               ("tweet", "TEXT"),
               ("tweet_favorite_count", "INT"),
               ("lang", "VARCHAR(255)"),
               ("retweet_count", "INT"),
               ("retweeted", "BOOL")]

    def requires(self):
        return TwitterInput()

    def rows(self):
        with self.input().open('r') as fobj:
            for line in fobj:
                columns = line.strip('\n').split('\t')
                # skip the header line
                if columns[0] == 'id':
                    continue
                if len(columns) != 7:
                    continue
                created_at_timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.strptime(columns[1],'%a %b %d %H:%M:%S +0000 %Y'))
                cols = [int(columns[0]), created_at_timestamp, columns[2], int(columns[3]), columns[4], int(columns[5]), bool(columns[6])]
                yield cols
