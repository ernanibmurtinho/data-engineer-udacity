import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS song_data.events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_data.songs;"
songplay_table_drop = "DROP TABLE IF EXISTS song_data.songplay;"
user_table_drop = "DROP TABLE IF EXISTS song_data.user;"
song_table_drop = "DROP TABLE IF EXISTS song_data.song;"
artist_table_drop = "DROP TABLE IF EXISTS song_data.artist;"
time_table_drop = "DROP TABLE IF EXISTS song_data.time;"

# CREATE TABLES


staging_events_table_create= ("""
CREATE TABLE song_data.events (
artist VARCHAR,
firstName VARCHAR,
gender VARCHAR,
itemInSession BIGINT,
lastName VARCHAR,
length VARCHAR,
level VARCHAR,
location VARCHAR,
sessionId BIGINT,
song VARCHAR,
status BIGINT,
ts VARCHAR,
userAgent VARCHAR,
userId INTEGER
)
""")


staging_songs_table_create = ("""
CREATE TABLE song_data.songs (
num_songs INTEGER,
artist_id VARCHAR,
artist_latitude VARCHAR,
artist_longitude VARCHAR,
artist_location VARCHAR,
artist_name VARCHAR,
song_id VARCHAR,
title VARCHAR,
duration VARCHAR,
year INTEGER
)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS song_data.users(
user_id VARCHAR PRIMARY KEY SORTKEY DISTKEY, 
first_name VARCHAR, 
last_name VARCHAR, 
gender CHAR, 
level VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_data.songs(
song_id VARCHAR PRIMARY KEY SORTKEY DISTKEY, 
title VARCHAR, 
artist_id VARCHAR, 
year VARCHAR, 
duration VARCHAR,
CONSTRAINT fk_artist_id
FOREIGN KEY(artist_id) 
REFERENCES song_data.artists(artist_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS song_data.artists(
artist_id VARCHAR PRIMARY KEY SORTKEY DISTKEY NOT NULL, 
artist_name VARCHAR NOT NULL, 
location VARCHAR, 
latitude VARCHAR, 
longitude VARCHAR
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS song_data.time(
start_time BIGINT PRIMARY KEY SORTKEY DISTKEY, 
hour BIGINT NOT NULL, 
day BIGINT NOT NULL, 
week BIGINT NOT NULL, 
month BIGINT NOT NULL, 
year BIGINT NOT NULL, 
weekday BIGINT NOT NULL
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS song_data.songplays( 
songplay_id INTEGER IDENTITY(0,1) SORTKEY DISTKEY, 
start_time VARCHAR NOT NULL, 
user_id VARCHAR NOT NULL, 
level VARCHAR, 
song_id VARCHAR, 
artist_id VARCHAR, 
session_id VARCHAR, 
location VARCHAR, 
user_agent VARCHAR
)
""")


# STAGING TABLES

staging_events_copy = ['song_data', 'events', 'log_data']

staging_songs_copy = ['song_data', 'songs', 'song_data']


# FINAL TABLES


artist_select = (""" SELECT \
                     COALESCE( artist_id, '0' ) artist_id, COALESCE(artist_name, 'N/A' ) artist_name, artist_location, \
                     artist_longitude, artist_latitude \
                     FROM song_data.songs
                 """)

song_select = (""" SELECT \
                   COALESCE(song_id, '0' ) song_id, title, artist_id, year, duration \
                   FROM song_data.songs
               """)

users_select = (""" SELECT \
                    COALESCE(userId, '0' ) userId, firstName, lastName, gender, level \
                    FROM song_data.events
                """)


time_select = (""" SELECT start_time,  EXTRACT(hour from start_time_f) AS hour, EXTRACT(day from start_time_f) AS day, \
                   EXTRACT(week from start_time_f) AS week, \
                   EXTRACT(month from start_time_f) AS month, \
                   EXTRACT(year from start_time_f) AS year, \
                   EXTRACT(dow from start_time_f) AS weekday \
                   FROM ( \
                   SELECT \
                   cast( timestamp 'epoch' + cast(ts as bigint)/1000 * interval '1 second' as timestamp) AS start_time_f, cast(ts as bigint) AS start_time
                   FROM song_data.events )
               """)

songplay_select = (""" SELECT \
                       evt.ts AS start_time, \
                       COALESCE(evt.userId, '0' ) AS user_id, \
                       evt.level, \
                       COALESCE(sgs.song_id, '0' ) AS song_id, \
                       COALESCE( sgs.artist_id, '0' ) AS artist_id, \
                       evt.sessionId AS session_id, \
                       evt.location, \
                       evt.userAgent AS user_agent \
                       FROM song_data.songs sgs  \
                       JOIN song_data.events evt ON evt.artist = sgs.artist_name \
                       AND evt.song = sgs.title
                   """)


songplay_table_insert = ("""
INSERT INTO song_data.songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
( {} );
""").format(songplay_select)

user_table_insert = ("""
INSERT INTO song_data.users (user_id, first_name, last_name, gender, level)
 ( {} );
""").format(users_select)

song_table_insert = ("""
INSERT INTO song_data.songs (song_id, title, artist_id, year, duration)
 ( {} );
""").format(song_select)

    
artist_table_insert = ("""
INSERT INTO song_data.artists (artist_id, artist_name, location, latitude, longitude)
 ( {} ) ;
""").format(artist_select)


time_table_insert = ("""
INSERT INTO song_data.time (start_time, hour, day, week, month, year, weekday)
( {} );
""").format(time_select)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
staging_songs_copy = ['song_data', 'songs', 'song_data']
staging_events_copy = ['song_data', 'events', 'log_data']
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
insert_table = ['user', 'song', 'artist', 'time', 'songplay']
