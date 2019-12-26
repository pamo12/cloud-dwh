import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('config/dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays;"
user_table_drop = "DROP TABLE IF EXISTS dim_users;"
song_table_drop = "DROP TABLE IF EXISTS dim_songs;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE stage_events (
 artist VARCHAR,
 auth VARCHAR,
 firstName VARCHAR,
 gender VARCHAR,
 itemInSession INT,
 lastName VARCHAR,
 length DECIMAL(12,5),
 level VARCHAR,
 location VARCHAR,
 method VARCHAR,
 page VARCHAR,
 registration BIGINT,
 sessionId BIGINT,
 song VARCHAR,
 status INT,
 ts BIGINT,
 userAgent TEXT,
 userId VARCHAR
);
""")

staging_songs_table_create = ("""
CREATE TABLE stage_songs (
 num_songs INT,
 artist_id VARCHAR,
 artist_latitude DECIMAL(12,5),
 artist_longitude DECIMAL(12,5),
 artist_name VARCHAR,
 song_id VARCHAR,
 title VARCHAR,
 duration DECIMAL,
 year INT
);
""")

songplay_table_create = ("""
CREATE TABLE fact_songplays (
 songplay_id INT IDENTITY(0,1) PRIMARY KEY,
 start_time BIGINT NOT NULL,
 user_id VARCHAR NOT NULL,
 level VARCHAR,
 song_id VARCHAR NOT NULL DISTKEY,
 artist_id VARCHAR NOT NULL,
 session_id VARCHAR,
 location VARCHAR,
 user_agent TEXT
);
""")

user_table_create = ("""
CREATE TABLE dim_users (
 user_id VARCHAR NOT NULL PRIMARY KEY SORTKEY,
 first_name VARCHAR,
 last_name VARCHAR,
 gender CHAR(2),
 level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE dim_songs (
 song_id VARCHAR NOT NULL PRIMARY KEY SORTKEY,
 title VARCHAR,
 artist_id VARCHAR NOT NULL,
 year INT,
 duration NUMERIC
);
""")

artist_table_create = ("""
CREATE TABLE dim_artists (
 artist_id VARCHAR NOT NULL PRIMARY KEY SORTKEY,
 name VARCHAR,
 location VARCHAR,
 latitude NUMERIC,
 longitude NUMERIC
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE dim_time (
 start_time BIGINT NOT NULL PRIMARY KEY SORTKEY,
 hour INT,
 day INT,
 week INT,
 month INT,
 year INT,
 weekday INT
) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
copy stage_events from {}
    credentials 'aws_iam_role={}'
    format as json {}
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy stage_songs from {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO fact_songplays
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
 DISTINCT ts AS start_time,
 se.userId AS user_id, 
 se.level,
 so.song_id,
 so.artist_id,
 se.sessionID AS session_id,
 se.location,
 se.useragent AS user_agent
FROM stage_events se
JOIN stage_songs so ON se.song = so.title AND se.artist = so.artist_name
WHERE se.page = 'NextSong' 
""")

user_table_insert = ("""INSERT INTO dim_users
(user_id, first_name, last_name, gender, level)
SELECT
 DISTINCT userid AS user_id,
 firstname AS first_name,
 lastname AS last_name,
 gender, 
 level
FROM stage_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""INSERT INTO dim_songs
 (song_id, title, artist_id, year, duration)
SELECT DISTINCT 
 song_id,
 title,
 artist_id,
 year,
 duration
FROM stage_songs
""")

artist_table_insert = ("""INSERT INTO dim_artists
(artist_id, name, location, latitude, longitude)
SELECT DISTINCT 
 so.artist_id,
 so.artist_name AS name,
 se.location,
 so.artist_latitude AS latitude,
 so.artist_longitude AS longitude
FROM stage_events se
JOIN stage_songs so ON se.song = so.title AND se.artist = so.artist_name
WHERE se.page = 'NextSong' 
""")

time_table_insert = ("""INSERT INTO dim_time
(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
 ts AS start_time,
 EXTRACT(hour FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS hour,
 EXTRACT(day FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS day,
 EXTRACT(week FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS week,
 EXTRACT(month FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS month,
 EXTRACT(year FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS year,
 EXTRACT(dow FROM TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') AS weekday
FROM stage_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
