import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
ARN = config.get('IAM_ROLE', 'ARN')

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
        gender VARCHAR(1),
        itemInSession INT,
        lastName VARCHAR,
        length FLOAT,
        level VARCHAR,
        location TEXT,
        method VARCHAR,
        page VARCHAR,
        registration VARCHAR,
        sessionId INT,
        song VARCHAR,
        status INT,
        ts BIGINT,
        userAgent TEXT,
        userId INT
        )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id         VARCHAR,
        num_songs       INT,
        artist_id       VARCHAR,
        artist_latitude  FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name     VARCHAR,
        title           VARCHAR,
        duration        FLOAT,
        year            INT
        )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INT NOT NULL,
        level VARCHAR,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id INT,
        location TEXT,
        user_agent TEXT
        )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender CHAR(1),
        level VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR,
        year INT,
        duration FLOAT
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        location TEXT ,
        latitude FLOAT ,
        longitude FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday VARCHAR
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    iam_role '{}'
    FORMAT AS JSON {}
    REGION 'us-west-2'
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    iam_role '{}'
    FORMAT AS JSON 'auto'
    REGION 'us-west-2'
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT DISTINCT TIMESTAMP 'epoch' + (se.ts / 1000) * INTERVAL '1 second' as start_time, se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
    FROM staging_events AS se
    LEFT JOIN staging_songs AS ss
    ON se.song = ss.title
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT se.userId, se.firstName, se.lastName, se.gender, se.level 
    FROM staging_events AS se
    WHERE se.page = 'NextSong' AND se.userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
    FROM staging_songs AS ss
    WHERE ss.song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude, ss.artist_longitude
    FROM staging_songs AS ss
    WHERE ss.artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time, 
        EXTRACT(hour from start_time),
        EXTRACT(day from start_time),
        EXTRACT(week from start_time),
        EXTRACT(month from start_time),
        EXTRACT(year from start_time),
        EXTRACT(weekday from start_time)
    FROM songplays
    WHERE start_time IS NOT NULL;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
