import luigi
from luigi import postgres

from constants import *
from twitter_input import TwitterInput


class UserDataToDb(luigi.postgres.CopyToTable):
    host = POSTGRES_HOST
    database = POSTGRES_DATABASE
    user = POSTGRES_USER
    password = POSTGRES_PASSWORD
    table = "users"

    columns = [(ID, "BIGINT"),
               (NAME, "VARCHAR(255)"),
               (DESCRIPTION, "TEXT"),
               (FAVORITE_COUNT, "INT"),
               (FOLLOWING, "BOOL"),
               (FOLLOWERS_COUNT, "INT"),
               (LOCATION, "VARCHAR(255)")]

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

                try:
                    # map tsv file columns with the table columns
                    database_columns = [int(columns[11]),
                                        columns[12],
                                        columns[13],
                                        int(columns[14]),
                                        bool(columns[15]),
                                        int(columns[16]),
                                        columns[17]]
                except ValueError:
                    # if there are conversion errors ignore that record
                    continue

                yield database_columns
