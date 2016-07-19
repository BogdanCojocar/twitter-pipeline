import luigi


class TwitterInput(luigi.ExternalTask):

    date = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget(self.date.strftime("raw-data/tweets_%d-%m-%Y.tsv"))

