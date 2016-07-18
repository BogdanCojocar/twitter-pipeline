import luigi.postgres


class TweetDataToDb(luigi.postgres.CopyToTable):

    def requires(self):
        return ""