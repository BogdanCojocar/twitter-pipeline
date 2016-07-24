import luigi
from luigi import postgres

from constants import *
from twitter_input import TwitterInput


class PlaceDataToDb(luigi.postgres.CopyToTable):
    host = POSTGRES_HOST
    database = POSTGRES_DATABASE
    user = POSTGRES_USER
    password = POSTGRES_PASSWORD
    table = "place"

    columns = [(ID, "VARCHAR(50)"),
               (COUNTRY, "VARCHAR(255)"),
               (COUNTRY_CODE, "VARCHAR(255)"),
               (NAME, "VARCHAR(255)")]

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
                    database_columns = [int(columns[7]),
                                        columns[8],
                                        columns[9],
                                        int(columns[10])]
                except ValueError:
                    # if there are conversion errors ignore that record
                    continue

                yield database_columns
