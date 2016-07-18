import luigi
from twitter_input import TwitterInput

if __name__ == '__main__':
    luigi.run(['TwitterInput', '--workers', '1', '--local-scheduler'])