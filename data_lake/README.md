# Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

# Project Description

The goal of the project is build an ELT pipeline that extracts the data from S3, process the data into analytics tables using Spark, and load them back into S3.

# Project DataSet

1. Song DataSet, each file is in JSON format and contains metadata about a song and the artist of that song
2. Log DataSet, the file has simulated app activity logs from an imaginary music streaming app based on configuration settings.

# Table schema

### Fact Table

#### songplays - records in log data associated with song plays i.e. records with page NextSong

table has the following columns: 
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

#### users - users in the app
    
table has the following columns:
user_id, first_name, last_name, gender, level


#### songs - songs in music database

table has the following columns:
song_id, title, artist_id, year, duration

#### artists - artists in music database

table has the following columns:
artist_id, name, location, lattitude, longitude

#### time - timestamps of records in songplays broken down into specific units

table has the following columns:
start_time, hour, day, week, month, year, weekday

# Files

1. etl.py, python script that read json files from S3, and then use pyspark to map it to fact and dimension table with appropriate data types, and then write the tables as parquet files back to S3.
2. dl.cfg, configuration file

# ETL steps

1. update the credentials from dl.cfg, make sure the role has write permission to the target s3 bucket
2. execute, python etl.py, which load the json files from udacity s3 bucket, and use spark to generate the fact and dimension table, and store it to s3 in parquet format.