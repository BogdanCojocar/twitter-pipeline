CREATE TABLE users (
  id INT NOT NULL,
  name VARCHAR(255),
  screen_name VARCHAR(255),
  description VARCHAR(255),
  favorites_count INT,
  follow_request_sent BOOLEAN,
  following BOOLEAN,
  followers_count INT,
  location VARCHAR(255),
  PRIMARY KEY(id)
);
