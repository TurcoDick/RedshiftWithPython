import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop    = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop     = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop          = "DROP TABLE IF EXISTS songsplays;"
user_table_drop              = "DROP TABLE IF EXISTS users;"
song_table_drop              = "DROP TABLE IF EXISTS songs;"
artist_table_drop            = "DROP TABLE IF EXISTS artists;"
time_table_drop              = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE staging_events(
    event_id int8 IDENTITY(0,1),
    artist_name VARCHAR(255),
    auth VARCHAR(50),
    user_first_name VARCHAR(255),
    user_gender  VARCHAR(1),
    item_in_session	INTEGER,
    user_last_name VARCHAR(255),
    song_length	DOUBLE PRECISION, 
    user_level VARCHAR(50),
    location VARCHAR(255),
    method VARCHAR(25),
    page VARCHAR(35),
    registration VARCHAR(50),
    session_id	BIGINT,
    song_title VARCHAR(255),
    status INTEGER,
    ts TIMESTAMP,
    user_agent TEXT,
    user_id VARCHAR(100))
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs int,
    artist_id text,
    artist_latitude float,
    artist_longitude float,
    artist_location text,
    artist_name varchar(400),
    song_id text,
    title text,
    duration float,
    year int)
""")



songplay_table_create = (""" CREATE TABLE IF NOT EXISTS songsplays(songplay_id int8 IDENTITY(0,1), \
                                                                  user_id varchar, \
                                                                  start_time varchar NOT NULL distkey, \
                                                                  level varchar, \
                                                                  song_id varchar, \
                                                                  artist_id varchar , \
                                                                  session_id varchar , \
                                                                  location varchar, \
                                                                  user_agent varchar , \
                                                                  PRIMARY KEY (songplay_id));""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id varchar distkey, \
                                                         first_name varchar, \
                                                         last_name varchar, \
                                                         gender varchar, \
                                                         level varchar, \
                                                         PRIMARY KEY(user_id));""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id varchar, \
                                                         song varchar NOT NULL distkey, \
                                                         artist_id varchar, \
                                                         year int, \
                                                         duration numeric, \
                                                         PRIMARY KEY(song_id));""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id text , \
                                                             artist_name varchar NOT NULL distkey, \
                                                             artist_location text, \
                                                             artist_latitude float, \
                                                             artist_longitude float, \
                                                             PRIMARY KEY(artist_id));""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(time_id int8 IDENTITY(0,1), \
                                                        start_time time NOT NULL distkey,\
                                                        hour integer, \
                                                        day integer, \
                                                        week integer, \
                                                        month integer, \
                                                        year integer, \
                                                        weekday integer, \
                                                        PRIMARY KEY(time_id));""")

# STAGING TABLES

try:
    staging_events_copy = ("""copy staging_events 
                                from {}
                                credentials  
                                'aws_iam_role={}'  
                                compupdate off
                                region 'us-west-2'
                                json{}
                                timeformat as 'epochmillisecs';
                            """).format(
                                        config.get("S3","LOG_DATA"),
                                        config.get("IAM_ROLE", "ARN"),
                                        config.get("S3","LOG_JSONPATH")
                                        )
except Exception as e:
    print(e)

    
try:    
    staging_songs_copy = ("""copy staging_songs
                                from {}
                                credentials
                                'aws_iam_role={}'
                                compupdate off
                                region 'us-west-2'
                                JSON 'auto';
                         """).format(
                                    config.get("S3", "SONG_DATA"),
                                    config.get("IAM_ROLE", "ARN")
                                    )
except Exception as e:
    print(e)    
    

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songsplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                                    SELECT DISTINCT events.ts,
                                     events.user_id,
                                     events.user_level,
                                     songs.song_id,
                                     songs.artist_id,
                                     events.session_id,
                                     events.location,
                                     events.user_agent
                            FROM staging_events AS events
                            JOIN staging_songs AS songs
                            ON events.artist_name = songs.artist_name
                            AND events.song_title = songs.title
                            AND events.song_length = songs.duration
                            WHERE events.page = 'NextSong';  
""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
                                  SELECT DISTINCT user_id, user_first_name, user_last_name, user_gender, user_level 
                                  FROM staging_events WHERE staging_events.user_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists(artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
                                       SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                                       FROM staging_songs;                                      
""")

song_table_insert = ("""INSERT INTO songs(song_id, song, artist_id, year, duration)
                                   SELECT DISTINCT song_id, title, artist_id, year, duration 
                                   from staging_songs;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                        SELECT DISTINCT
                            CAST(ts AS time) as TIME,
                            EXTRACT (HOUR FROM ts) as HOUR,
                            EXTRACT (DAY FROM ts) AS DAY,
                            EXTRACT (WEEK FROM ts) AS WEEK,
                            EXTRACT (MONTH FROM ts) AS MONTH,
                            EXTRACT (YEAR FROM ts) AS YEAR,
                            DATE_PART(dow, ts) AS WEEKDAY
                            FROM staging_events;  
                        
""")


# QUERY TABLES

query_staging_events    = ("""SELECT * FROM staging_events LIMIT 2;""")
query_staging_songs     = ("""SELECT * FROM staging_songs LIMIT 2;""")
query_users             = ("""SELECT * FROM users LIMIT 2;""")
query_artists           = ("""SELECT * FROM artists LIMIT 2;""")
query_time              = ("""SELECT * FROM time LIMIT 2;""")
query_songs             = ("""SELECT * FROM songs LIMIT 2;""")
query_songsplays        = ("""SELECT * FROM songsplays LIMIT 2;""")


# QUERY LISTS 

create_table_queries     = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries       = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries       = [staging_events_copy, staging_songs_copy]
insert_table_queries     = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]  #
query_tables             = [query_staging_events, query_staging_songs, query_users, query_artists, query_time, query_songs, query_songsplays]  # 
