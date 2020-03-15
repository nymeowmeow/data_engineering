import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY                 = config.get('AWS','KEY')
SECRET              = config.get('AWS','SECRET')
HOST                = config.get('CLUSTER','HOST')
IAM_ARN             = config.get('IAM_ROLE','ARN')
REGION              = config.get('AWS_SITE', 'REGION')
LOG_DATA            = config.get('S3','LOG_DATA')
LOG_JSONPATH        = config.get('S3','LOG_JSONPATH')
SONG_DATA           = config.get('S3','SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender CHAR(1),
itemInSession INT,
lastName VARCHAR,
length DECIMAL,
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration DECIMAL,
sessionId INT,
song VARCHAR,
status INT,
ts TIMESTAMP,
userAgent VARCHAR,
userId INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
(
num_songs INT,
artist_id VARCHAR,
artist_latitude DECIMAL(9,6),
artist_longitude DECIMAL(9,6),
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration DECIMAL,
year INT                                                                       
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
songplay_id INT IDENTITY(0,1) PRIMARY KEY,
start_time TIMESTAMP REFERENCES time (start_time) distkey sortkey,
user_id INT REFERENCES users (user_id),
level VARCHAR NOT NULL,
song_id VARCHAR REFERENCES songs (song_id),
artist_id VARCHAR REFERENCES artists (artist_id),
session_id INT NOT NULL,
location TEXT,
user_agent TEXT
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id INT PRIMARY KEY,
first_name VARCHAR,
last_name VARCHAR,
gender CHAR(1),
level VARCHAR NOT NULL
)
COMPOUND SORTKEY(last_name, first_name);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
song_id VARCHAR PRIMARY KEY,
title VARCHAR NOT NULL,
artist_id VARCHAR REFERENCES artists (artist_id),
year INT,
duration DECIMAL
)
COMPOUND SORTKEY(artist_id, song_id, year, title);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id VARCHAR PRIMARY KEY,
name VARCHAR,
location VARCHAR,
latitude DECIMAL(9,6),
longitude DECIMAL(9,6)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
start_time TIMESTAMP PRIMARY KEY distkey sortkey,
hour INT NOT NULL,
day INT NOT NULL,
week INT NOT NULL,
month INT NOT NULL,
year INT NOT NULL,
weekday VARCHAR NOT NULL
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events (artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId)
FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION '{}'
TIMEFORMAT as 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ARN[1:-1], REGION[1:-1], LOG_JSONPATH)

staging_songs_copy = ("""
COPY staging_songs (num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year)
FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE off
REGION '{}'
FORMAT AS JSON 'auto'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, IAM_ARN[1:-1], REGION[1:-1])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT(e.ts) AS start_time,
       e.userid AS user_id,
       e.level,
       s.song_id,
       s.artist_id,
       e.sessionid as session_id,
       e.location,
       e.useragent AS user_agent
FROM staging_events e
JOIN staging_songs s
ON (e.song = s.title AND e.artist = s.artist_name)
WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT(userid) AS user_id,
       firstname AS first_name,
       lastname AS last_name,
       gender,
       level
FROM staging_events
WHERE userid IS NOT NULL AND
      ts = (SELECT MAX(ts) FROM staging_events where user_id = userId);
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT(song_id),
title,
artist_id,
year,
duration
FROM staging_songs
WHERE song_id IS NOT NULL
      AND artist_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists
(artist_id, name, location, latitude, longitude)
SELECT distinct(artist_id) AS artist_id,
       artist_name      AS name,
       artist_location  AS location,
       artist_latitude  AS latitude,
       artist_longitude AS longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
      AND artist_name is not NULL;
""")

time_table_insert = ("""
INSERT INTO time
(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT ts as start_time,
       EXTRACT(hour    FROM ts) AS hour,
       EXTRACT(day     FROM ts) AS day,
       EXTRACT(week    FROM ts) AS week,
       EXTRACT(month   FROM ts) AS month,
       EXTRACT(year    FROM ts) AS year,
       EXTRACT(weekday FROM ts) AS weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, artist_table_insert, song_table_insert, time_table_insert, songplay_table_insert]