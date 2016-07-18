CREATE TABLE tweet_hashtags (
  tweet_id INT REFERENCES tweet(id) ON UPDATE CASCADE ON DELETE NO ACTION,
  hashtag_id INT REFERENCES hashtags(id) ON UPDATE CASCADE ON DELETE NO ACTION,
  CONSTRAINT tweet_hashtags_pkey PRIMARY KEY(tweet_id, hashtag_id)
);
