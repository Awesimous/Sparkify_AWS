import configparser


# CONFIG
config = configparser.ConfigParser()
# config.read('dwh.cfg')
config.read_file(open('dwh.cfg'))
KEY=config.get('AWS','KEY')
SECRET= config.get('AWS','SECRET')

DWH_ROLE_ARN = config.get('IAM_ROLE','ARN')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table CASCADE"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table CASCADE"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

staging_events_table_create= ("""
                              CREATE TABLE staging_events_table (
                                  artist VARCHAR,
                                  auth VARCHAR,
                                  firstName VARCHAR,
                                  gender VARCHAR(1),
                                  itemInSession INT,
                                  lastName VARCHAR,
                                  length FLOAT,
                                  level VARCHAR,
                                  location VARCHAR,
                                  method VARCHAR,
                                  page VARCHAR,
                                  registration FLOAT,
                                  sessionId INT,
                                  song VARCHAR,
                                  status INT,
                                  ts BIGINT,
                                  userAgent VARCHAR,
                                  userId INT)
""")

staging_songs_table_create = ("""
                              CREATE TABLE staging_songs_table (
                                  num_songs INT,
                                  artist_id VARCHAR,
                                  artist_latitude FLOAT,
                                  artist_longitude FLOAT,
                                  artist_location VARCHAR,
                                  artist_name VARCHAR,
                                  song_id VARCHAR,
                                  title VARCHAR,
                                  duration FLOAT,
                                  year INT)
""")

songplay_table_create = ("""
                              CREATE TABLE songplays (
                                  songplay_id INT IDENTITY(0,1) PRIMARY KEY, 
                                  start_time TIMESTAMP NOT NULL REFERENCES time(start_time), 
                                  user_id INT NOT NULL NOT NULL REFERENCES users(user_id), 
                                  level VARCHAR,
                                  song_id VARCHAR NOT NULL REFERENCES songs (song_id),
                                  artist_id VARCHAR NOT NULL REFERENCES artists (artist_id),
                                  session_id INT,
                                  location VARCHAR,
                                  user_agent VARCHAR)
""")

user_table_create = ("""
                              CREATE TABLE users (
                                  user_id INT PRIMARY KEY,
                                  first_name VARCHAR NOT NULL,
                                  last_name VARCHAR NOT NULL,
                                  gender VARCHAR(1),
                                  level VARCHAR)
""")

song_table_create = ("""
                              CREATE TABLE songs (
                                  song_id VARCHAR PRIMARY KEY,
                                  title VARCHAR,
                                  artist_id VARCHAR,
                                  year INT,
                                  duration NUMERIC)
""")

artist_table_create = ("""
                              CREATE TABLE artists (
                                  artist_id VARCHAR PRIMARY KEY,
                                  name VARCHAR,
                                  location VARCHAR,
                                  lattitude FLOAT,
                                  longitude FLOAT)
""")

time_table_create = ("""
                              CREATE TABLE time (
                                  start_time TIMESTAMP PRIMARY KEY,
                                  hour INT,
                                  day INT,
                                  week INT,
                                  month INT,
                                  year INT,
                                  weekday INT)
""")

# STAGING TABLES

staging_events_copy = ("""
                       copy staging_events_table from {} 
                       iam_role {}
                       region 'us-west-2'
                       json {};
""").format(config['S3']['LOG_DATA'], DWH_ROLE_ARN, config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
                       copy staging_songs_table from {}
                       iam_role {}
                       region 'us-west-2'
                       json 'auto'
                       maxerror as 5;
""").format(config['S3']['SONG_DATA'], DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                         SELECT DISTINCT(TIMESTAMP 'epoch' + INTERVAL '1 second' * ts/1000) AS e.start_time,
                         e.userId AS user_id,
                         e.level,
                         s.song_id AS song_id,
                         s.artist_id AS artist_id,
                         e.sessionId AS session_id,
                         e.location,
                         e.userAgent AS user_agent
                         FROM staging_events_table e
                         JOIN staging_songs_table s ON (e.song = s.title AND e.artist = s.artist_name)
                         WHERE e.page = 'NextSong'
                         
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                         SELECT userId AS user_id, 
                         firstName AS first_name,
                         lastName AS last_name,
                         gender,
                         level
                         FROM staging_events_table
                         WHERE user_id IS NOT NULL
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                         SELECT song_id,
                         title,
                         artist_id,
                         year,
                         duration
                         FROM staging_songs_table
                         WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude)
                         SELECT artist_id
                         artist_name AS name,
                         artist_location AS location,
                         artist_lattitude,
                         artist_longitude
                         FROM staging_songs_table
                         WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""INSERT INTO times (start_time, hour, day, week, month, year, weekday)
                         SELECT DISTINCT(TIMESTAMP 'epoch' + INTERVAL '1 second' * ts/1000) AS start_time,
                         EXTRACT(hour FROM start_time)                              AS hour,
                         EXTRACT(day FROM start_time)                             AS day,
                         EXTRACT(week FROM start_time)                               AS week,
                         EXTRACT(month FROM start_time)                              AS month,
                         EXTRACT(year FROM start_time)                               AS year,
                         EXTRACT(ISODOW FROM start_time)                              AS weekday,                      
                         FROM staging_events_table
""")

# QUERY LISTS
drop_table_queries = {
    'staging_events_table' : staging_events_table_drop,
    'staging_songs_table' : staging_songs_table_drop,
    'user_table' : user_table_drop,
    'song_table' : song_table_drop,
    'artist_table' : artist_table_drop,
    'time_table' : time_table_drop,
    'songplay_table' : songplay_table_drop
}

create_table_queries = {
    'staging_events_table' : staging_events_table_create,
    'staging_songs_table' : staging_songs_table_create,
    'user_table' : user_table_create,
    'song_table' : song_table_create,
    'artist_table' : artist_table_create,
    'time_table' : time_table_create,
    'songplay_table' : songplay_table_create
}

copy_table_queries = {
    'staging_events_table' : staging_events_copy,
    'staging_songs_table' : staging_songs_copy
}

insert_table_queries = {
    'songplay_table' : songplay_table_insert,
    'user_table' : user_table_insert,
    'song_table' : song_table_insert,
    'artist_table' : artist_table_insert,
    'time_table' : time_table_insert
}

# staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
# copy_table_queries = [staging_events_copy, staging_songs_copy]
# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
