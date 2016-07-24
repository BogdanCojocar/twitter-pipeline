import time

import luigi
from luigi import postgres

from constants import *
from twitter_input import TwitterInput


class TweetDataToDb(luigi.postgres.CopyToTable):
    host = POSTGRES_HOST
    database = POSTGRES_DATABASE
    user = POSTGRES_USER
    password = POSTGRES_PASSWORD
    table = "tweet"

    columns = [(ID, "BIGINT"),
               (PLACE_ID, "VARCHAR(50)")
               (USER_ID, "BIGINT")
               (CREATED_AT, "TIMESTAMP WITHOUT TIME ZONE"),
               (TEXT, "TEXT"),
               (FAVORITE_COUNT, "INT"),
               (LANG, "VARCHAR(255)"),
               (RETWEET_COUNT, "INT"),
               (RETWEETED, "BOOL")]

    def requires(self):
        return TwitterInput()

    def rows(self):
        with self.input().open('r') as fobj:
            for line in fobj:
                columns = line.strip('\n').split('\t')
                # skip the header line
                if columns[0] == ID:
                    continue
                if len(columns) != TSV_NUMBER_OF_FIELDS:
                    continue

                created_at_timestamp = time.strftime('%Y-%m-%d %H:%M:%S',
                                                     time.strptime(columns[1],
                                                     '%a %b %d %H:%M:%S +0000 %Y'))

                try:
                    # map tsv file columns with the table columns
                    database_columns = [int(columns[0]),
                                        columns[7],
                                        int(columns[11]),
                                        created_at_timestamp,
                                        columns[2],
                                        int(columns[3]),
                                        columns[4],
                                        int(columns[5]),
                                        bool(columns[6])]
                except ValueError:
                    # if there are conversion errors ignore that record
                    continue

                yield database_columns
