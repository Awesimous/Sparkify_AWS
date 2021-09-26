# Introduction
## Sparkify

Sparkify strives for a world with more music!
How do we do this? By accompanying you with the best-fit for your taste. We ensure that the music in your ears is just the right for you - whatever the situation and mood might be!

# Startup the project

The aim of the project is to empower our data analytics with the most effective structure possible, on the AWS Cloud!
For that reason, we build a ETL pipeline for extracting data from s3, stage them on Redshift and transform them into dimensional tables for song play analysis. 
With the Cloud approach we want data that:

- is easily accessible,
- has a structure that is easy to understand,
- can be analysed by SQL queries.

# Point of Origin

Our data is stored as 2 sets of json files:
## Log data

Log_data files store all information we have about users and their sessions, including user's name and location, level of access, song and artist name, timestamp when the song was played etc. The fields available in every log_data file are:

- artist
- auth
- firstName
- gender
- itemInSession
- lastName
- length
- level
- location
- method
- page
- registration
- sessionId
- song
- status
- ts
- userAgent
- userId

The log_data files are partitioned by year and month, with a separate folder for each partition. For example, below we have pasted filepaths to two files in this dataset:

- log_data/2018/11/2018-11-12-events.json
- log_data/2018/11/2018-11-13-events.json

## Song data

Song_data files provide information about every single songs available in our service, along with some info about the artist. The following fields are available for each song:

- artist_id
- artist_latitude
- artist_location
- artist_longitude
- artist_name
- duration
- num_songs
- song_id
- title
- year

Each json file in the song_data dataset stores info about one song. The song_data files are partitioned by the first three letters of each song's track ID. For example, below we have pasted filepaths to two files in this dataset:

- song_data/A/B/C/TRABCEI128F424C983.json
- song_data/A/A/B/TRAABJL12903CDCF1A.json 

# Database Design
<img width="806" alt="grafik" src="https://user-images.githubusercontent.com/29717043/134798326-eee23f4f-f145-4166-b586-552ff2f790b3.png">

## Fact table

__Table name: songplays__
Fields: songplay_id, start_time, user_id, level, session_id, location, user_agent, song_id, artist_id
Datasource: log_data, song_data
Dimensions

## Dimension tables

__Table name: users__
Fields: user_id, first_name, last_name, gender, level
Datasource: log_data

__Table name: songs__
Fields: song_id, title, artist_id, year, duration
Datasource: song_data

__Table name: artists__
Fields: artist_id, name, location, latitude, longitude
Datasource: song_data

__Table name: time__
Fields: start_time, hour, day, week, month, year, weekday
Datasource: log_data
Files in repository

## Explanation of files
__Files only work if a Cluster is up and running on Redshift__
The project workspace includes six files:

- create_tables.py drops and creates database tables. Resets tables prior to running the ETL- scripts.
- etl.py reads and processes files from song_data and log_data buckets stored in S3 and stages them on Redshift. Staged tables are subsequently used to insert data in our created data schema logic (see create_tables.py with respective sql queries). 
- sql_queries.py contains all the projects sql queries and refered to by create_tables.py and etl.py.
- main.py driver function to execute create_tables and etl subsequently

## How to run

__Run the drive program main.py__

- python main.py
- lean back

__The create_tables.py and etl.py file can also be run independently__

- python create_tables.py 
- python etl.py 
