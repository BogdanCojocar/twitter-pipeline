import luigi
from twitter_input import TwitterInput

if __name__ == '__main__':
    twitter_input = TwitterInput()
    twitter_input.start_stream()
    luigi.run(['TweetDataToDb', '--workers', '1', '--local-scheduler'])