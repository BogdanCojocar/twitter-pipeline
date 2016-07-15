CREATE TABLE tweet (
  id INT NOT NULL,
  place_id VARCHAR(50) REFERENCES place(id) ON UPDATE CASCADE ON DELETE NO ACTION,
  user_id INT REFERENCES users(id) ON UPDATE CASCADE ON DELETE NO ACTION,
  lang VARCHAR(255),
  tweet_favorite_count INT,
  retweet_count INT,
  retweeted BOOLEAN,
  tweet TEXT,
  PRIMARY KEY(id)
);
