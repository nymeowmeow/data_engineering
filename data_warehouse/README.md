# Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Project Description

The goal of the project is to load data from S3 to staging tables on Redshift and then execute SQL statements to create and populate the analytics tables from these staging tables.

# Table schema

### Staging Table, staging tables are to store the data from JSON files stored under S3.

#### staging_events - staging table to read in log dataset

| Column        | Type      |
| ------------- | --------- |
| artist        | VARCHAR   |
| auth          | VARCHAR   |
|firstName      | VARCHAR   |
| gender        | CHAR(1)   |
| itemInSession | INT       |
| lastName      | VARCHAR   |
| length        | DECIMAL   |
| level         | VARCHAR   |
| location      | VARCHAR   |
| method        | VARCHAR   |
| page          | VARCHAR   |
| registration  | DECIMAL   |
| sessionId     | INT       |
| song          | VARCHAR   |
| status        | INT       |
| ts            | TIMESTAMP |
| userAgent     | VARCHAR   |
| userId        | INT       |

copy statement:

COPY staging_events (artist, auth, firstName, gender, itemInSession, lastName, length, level, location, method, page, registration, sessionId, song, status, ts, userAgent, userId)
FROM {}
CREDENTIALS 'aws_iam_role={}'
REGION '{}'
TIMEFORMAT as 'epochmillisecs'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
FORMAT AS JSON {};
""").format(LOG_DATA, IAM_ARN[1:-1], REGION[1:-1], LOG_JSONPATH)

#### staging_songs - staging table to read in song dataset.

| Column           | Type         |
| ---------------- | ------------ |
| num_songs        | INT          |
| artist_id        | VARCHAR      |
| artist_latitude  | DECIMAL(9,6) |
| artist_longitude | DECIMAL(9,6) |
| artist_location  | VARCHAR      |
| artist_name      | VARCHAR      |
| song_id          | VARCHAR      |
| title            | VARCHAR      |
| duration         | DECIMAL      |
| year             | INT

copy statement:

COPY staging_songs (num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year)
FROM {}
CREDENTIALS 'aws_iam_role={}'
COMPUPDATE off
REGION '{}'
FORMAT AS JSON 'auto'
TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA, IAM_ARN[1:-1], REGION[1:-1])

### Star Schema ###
The data warehouse is designed using **Star Schema** consisting of fact and dimension tables. The star schema simplifies the overall data model, making business analytics much more easily expressed in SQL.

### Fact Table

#### songplays - records in log data associated with song plays i.e. records with page NextSong
start_time, users, songs and artists is setup as foreign key to the respective dimension table. Also we want to roughly have even distribution of data across the cluster, so using start_time will be a good choice for the dist_key, and it is expected a lot of queries will do a order by start_time also, so it is used as a sort key also.

| Column      | Type      | Constraint                    | 
| ----------- | --------- | ----------------------------- |
| songplay_id | IDENTITY  | PRIMARY KEY                   |
| start_time  | TIMESTAMP | REFERENCES time (start_time)  |
| user_id     | INT       | REFERENCES users (user_id)    |
| level       | VARCHAR   | NOT NULL                      |
| song_id     | VARCHAR   | REFERENCES songs (song_id)    |
| artist_id   | VARCHAR   | REFERENCES artists (artist_id)|
| session_id  | INT       | NOT NULL                      |
| location    | TEXT      |                               |
| user_agent  | TEXT      |                               |

distkey is start_time
sortkey is start_time
    
the insert statement is
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

### Dimension Tables

#### users - users in the app
    
| Column      | Type    | Constraint  |
| ----------- | ------- | ----------- |
| user_id     | INT     | PRIMARY KEY |
| first_name  | VARCHAR |             |
| last_name   | VARCHAR |             |
| gender      | CHAR(1) |             |
| level       | VARCHAR | NOT NULL    |

sortkey is first_name and last_name, most of th queries will have order by first and last name, so these columns are picked as sort key.

the insert statement is

INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT(userid) AS user_id,
       firstname AS first_name,
       lastname AS last_name,
       gender,
       level
FROM staging_events
WHERE userid IS NOT NULL AND
      ts = (SELECT MAX(ts) FROM staging_events where user_id = userId);

#### songs - songs in music database
artist_id is defined as foreign key to the artist table

| Column    | Type     | Constraint                     |
| --------- | -------- | ------------------------------ |
| song_id   | VARCHAR  | PRIMARY KEY                    |
| title     | VARCHAR  | NOT NULL                       |
| artist_id | VARCHAR  | REFERENCES artists (artist_id) |
| year      | INT      |                                |
| duration  | DECIMAL  |                                |

sortkey is artist_id, song_id, year and title

the insert statement is

INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT(song_id),
title,
artist_id,
year,
duration
FROM staging_songs
WHERE song_id IS NOT NULL
      AND artist_id IS NOT NULL

#### artists - artists in music database

| Column    | Type         | Constraint     |
| --------- | ------------ | -------------- |
| artist_id | VARCHAR      | PRIMARY KEY    |
| name      | VARCHAR      |                |
| location  | VARCHAR      |                |
| latitude  | DECIMAL(9,6) |                |
| longitude | DECIMAL(9,6) |                |

the insert statement is

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

#### time - timestamps of records in songplays broken down into specific units

| Column     | Type      | Constraint                                  |
| ---------- | --------- | ------------------------------------------- |
| start_time | TIMESTAMP | PRIMARY KEY                                 |
| hour       | INT       | NOT NULL                                    |
| day        | INT       | NOT NULL                                    |
| week       | INT       | NOT NULL                                    |
| month      | INT       | NOT NULL                                    |
| year       | INT       | NOT NULL                                    |
| weekday    | VARCHAR   | NOT NULL                                    |

distkey is start_time
sortkey is start_time

the insert statement is

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

# Files

1. etl.py, python script that performs the etl step, to load json files into the staging table and from the staging table to the proper redshift table. 
2. sql_queries.py, define function to create, drop the fact, staging and dimension tables, as well as insert and copy statements to the corresponding staging, fact and dimension tables.
3. create_tables.py, python script that invokes the functions defined in sql_queries.py to drop and create fact, staging and dimension tables.


# ETL Pipeline

1. run create_table.py to execute the sql to drop existing table and then create the fact, staging and dimension tables
2. execute the load_staging_tables function in etl.py to load song json files to staging tables.
3. execute the insert_tables in etl.py to load data in staging tables to the corresponding tables in star schema

# Execution Steps

1. python create_tables.py
2. python etl.py
3. run test.ipynb to verify the data stored in database is proper

# Samples queries
1. find the song with longest duration
select * from songs where duration = (select max(duration) from songs)

| song_id            | title                | artist_id          | year | duration |
|------------------- | -------------------- | ------------------ | ---- | -------- |
| SOQTXVQ12A8C13E6C4 | Chapter One: Destiny | ARRIWD31187B9A9B4A | 2003 | 2709     |

2. find the number of song play broken up by hour
select count(s.songplay_id), t.hour from songplays s join time t
on t.start_time = s.start_time group by t.hour order by t.hour

| count | hour |
| ----- | ---- |
| 6     | 0    |
| 11    | 1    |
| 3     | 2    |
| 2     | 3    |
| 7     | 4    |
| 7     | 5    |
| 9     | 6    |
| 13    | 7    |
| 18    | 8    |
| 9     | 9    |
| 11    | 10   |
| 16    | 11   |
| 12    | 12   |
| 14    | 13   |
| 16    | 14   |
| 25    | 15   |
| 24    | 16   |
| 40    | 17   |
| 26    | 18   |
| 16    | 19   |
| 18    | 20   |
| 12    | 21   |
| 7     | 22   |
| 11    | 23   |

