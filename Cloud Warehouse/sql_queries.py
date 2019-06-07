import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events";
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs";
songplay_table_drop = "DROP TABLE IF EXISTS songplay";
user_table_drop = "DROP TABLE IF EXISTS users";
song_table_drop = "DROP TABLE IF EXISTS song";
artist_table_drop = "DROP TABLE IF EXISTS artist";
time_table_drop = "DROP TABLE IF EXISTS time";

# CREATE TABLES

staging_events_table_create= ("""

CREATE TABLE "staging_events" (
"artist"character varying(1000),
"auth" character varying(20) ,
"firstname" character varying(20) ,
"gender" character varying(5) ,
"iteminsession" int ,
"lastname" character varying(10) ,
"length" float(8),
"level"  character varying(10) ,
"location" character varying(MAX) ,
"method" character varying(10) ,
"page" character varying(100) ,
"registration" float(3) ,
"sessionid" int ,
"song" character varying(500) ,
"status" int ,
"ts" timestamp ,
"useragent" character varying(MAX) ,
"userid" int );

""")

staging_songs_table_create = ("""

CREATE TABLE IF NOT EXISTS "staging_songs" (
"artist_id" character varying(max) ,
"artist_latitude" decimal(10,8) ,
"artist_location" character varying(max) ,
"artist_longitude" decimal (11,8) ,
"artist_name" character varying(max) ,
"song_id" character varying(max) ,
"title"  character varying(max) ,
"duration"character varying(max) ,
"num_song" int ,
"year" int );


""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay(
    songplay_id INT IDENTITY(0,1)  , 
    start_time timestamp, 
    user_id int, 
    level character varying(10), 
    song_id character varying(max), 
    artist_id character varying(max) , 
    session_id int, 
    location character varying(max), 
    user_agent character varying(max))
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
user_id INT  , 
first_name varchar(20), 
last_name varchar(20), 
gender varchar(5), 
level varchar(10))

""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song(song_id int IDENTITY(0,1), 
title character varying(max), 
artist_id character varying(max), 
year int, 
duration character varying(max))""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
artist_id character varying(max), 
    name character varying(max),
    location character varying(max), 
    latitude decimal(10,8) , 
    longitude decimal(11,8)  )""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time(
start_time timestamp primary key, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int )""")

# STAGING TABLES

staging_events_copy = ("""


copy staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2' FORMAT AS JSON {} timeformat AS 'epochmillisecs';


""").format(config.get("S3", "LOG_DATA"), ARN, config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""

copy staging_songs from ''
credentials 'aws_iam_role={}'
json 'auto';

""").format(ARN)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)                      
                      SELECT distinct  ts as  start_time,
                               userid as  user_id,
                               level as   level,
                               song_id as  song_id,
                               artist_id as  artist_id,
                               sessionid as  session_id,
                               location as  location,
                               useragent as  user_agent
                from staging_events  l inner join staging_songs e on e.title = l.song and artist_name = artist   
""")
user_table_insert = (""" INSERT INTO users(user_id, first_name, last_name, gender, level)
                 select distinct userid as user_id,
                 firstname as first_name,
                 lastname as last_name,
                 gender as gender,
                 level as level
                 from staging_events where userid is not null 
""")

song_table_insert = ("""INSERT INTO song( title,artist_id, year, duration) 
                select title as title,
                 artist_id as artist_id,
                 year as year,
                 duration as duration
                 from staging_songs              
                 
""")

artist_table_insert = ("""INSERT INTO  artists( artist_id ,name, location, latitude, longitude) 
              select   artist_id as artist_id,
                 artist_name as name,
                 artist_location as location,
                 artist_latitude as latitude,
                 artist_longitude as longitude
                 from staging_songs  
""")

time_table_insert = ("""INSERT INTO  time( start_time, hour, day, week, month, year, weekday) 
              select  ts as  start_time,
                extract (hr from ts ) as hour,
                 extract (d from ts ) as day,
                 extract (w from ts ) as week,
                 extract (mon from ts ) as month,
                 extract (yr from ts ) as year,
                 extract (dw from ts ) as weekday 
                 from staging_events
""")

# QUERY LISTS
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert , user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
