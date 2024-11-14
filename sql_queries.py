import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
SONG_DATA = config.get('S3', 'SONG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
  CREATE TABLE IF NOT EXISTS staging_events (
    artist        VARCHAR(200),
    auth          VARCHAR(200),
    firstName     VARCHAR(200),
    gender        VARCHAR(200),
    itemInSession INTEGER,
    lastName      VARCHAR(200),
    length        NUMERIC,
    level         VARCHAR(200),
    location      VARCHAR(200),
    method        VARCHAR(200),
    page          VARCHAR(200),
    registration  BIGINT,
    sessionId     INTEGER,
    song          VARCHAR(200),
    status        INTEGER,
    ts            BIGINT NOT NULL,
    userAgent     VARCHAR(200),
    userId        VARCHAR(200)
  );
""")

staging_songs_table_create = ("""
  CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs     INTEGER,
    artist_id     VARCHAR(200),
    artist_latitude   VARCHAR(200),
    artist_longitude  VARCHAR(200),
    artist_location   VARCHAR(200),
    artist_name   VARCHAR(200),
    song_id       VARCHAR(200),
    title         VARCHAR(200),
    duration      NUMERIC,
    year          INTEGER
  );
""")

songplay_table_create = ("""
  CREATE TABLE IF NOT EXISTS songplays (
    songplay_id   INTEGER IDENTITY(1,1) PRIMARY KEY,
    start_time    TIMESTAMP NOT NULL SORTKEY,
    user_id       VARCHAR(200) NOT NULL DISTKEY,
    level         VARCHAR(200),
    song_id       VARCHAR(200),
    artist_id     VARCHAR(200),
    session_id    INTEGER,
    location      VARCHAR(200),
    user_agent    VARCHAR(200)     
  ) diststyle key;
""")

user_table_create = ("""
  CREATE TABLE IF NOT EXISTS users (
    user_id     VARCHAR(200) PRIMARY KEY SORTKEY,
    first_name  VARCHAR(200),
    last_name   VARCHAR(200),
    gender      VARCHAR(200),
    level       VARCHAR(200)        
  ) diststyle all;
""")

song_table_create = ("""
  CREATE TABLE IF NOT EXISTS songs (
    song_id     VARCHAR(200) PRIMARY KEY SORTKEY,
    title       VARCHAR(200),
    artist_id   VARCHAR(200) DISTKEY,
    year        INTEGER,
    duration    NUMERIC
  ) diststyle key;
""")

artist_table_create = ("""
  CREATE TABLE IF NOT EXISTS artists (
    artist_id   VARCHAR(200) PRIMARY KEY SORTKEY,
    name        VARCHAR(200),
    location    VARCHAR(200),
    latitude    VARCHAR(200),
    longitude   VARCHAR(200)
  ) diststyle all;
""")

time_table_create = ("""
  CREATE TABLE IF NOT EXISTS time (
    start_time  TIMESTAMP NOT NULL PRIMARY KEY SORTKEY,
    hour        INTEGER NOT NULL,
    day         INTEGER NOT NULL,
    week        INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    year        INTEGER NOT NULL,
    weekday     INTEGER NOT NULL
  ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = (f"""
  COPY staging_events FROM {LOG_DATA}
  IAM_ROLE {DWH_ROLE_ARN}
  JSON {LOG_JSONPATH}
  REGION 'us-west-2';
""")

staging_songs_copy = (f"""
  COPY staging_songs FROM {SONG_DATA}
  IAM_ROLE {DWH_ROLE_ARN}
  JSON 'auto'
  REGION 'us-west-2';
""")

# FINAL TABLES

songplay_table_insert = ("""
  INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
  SELECT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' AS start_time,
         e.userId AS user_id,
         e.level AS level,
         s.song_id AS song_id,
         s.artist_id AS artist_id,
         e.sessionId AS session_id,
         e.location AS location,
         e.userAgent AS user_agent
  FROM staging_events e
  LEFT JOIN staging_songs s ON e.song = s.title AND
      e.artist = s.artist_name AND
      ABS(e.length - s.duration) < 2
  WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
  INSERT INTO users (user_id, first_name, last_name, gender, level)
  SELECT DISTINCT userId AS user_id,
         firstName AS first_name,
         lastName AS last_name,
         gender AS gender,
         level AS level
  FROM staging_events;
""")

song_table_insert = ("""
  INSERT INTO songs (song_id, title, artist_id, year, duration)
  SELECT DISTINCT song_id AS song_id,
         title AS title,
         artist_id AS artist_id,
         year AS year,
         duration AS duration
  FROM staging_songs;
""")

artist_table_insert = ("""
  INSERT INTO artists (artist_id, name, location, latitude, longitude)
  SELECT DISTINCT artist_id AS artist_id,
         artist_name AS name,
         artist_location AS location,
         artist_latitude AS latitude,
         artist_longitude AS longitude
  FROM staging_songs;
""")

time_table_insert = ("""
  INSERT INTO time (start_time, hour, day, week, month, year, weekday)
  SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
         EXTRACT(HOUR FROM (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS hour,
         EXTRACT(DAY FROM (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS day,
         EXTRACT(WEEK FROM (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS week,
         EXTRACT(MONTH FROM (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS month,
         EXTRACT(YEAR FROM (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS year,
         EXTRACT(DOW FROM (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) as weekday
  FROM staging_events;
""")

# COUNTING QUERIES
staging_events_count = "SELECT COUNT(*) FROM staging_events;"
staging_songs_count = "SELECT COUNT(*) FROM staging_songs;"
songplay_count = "SELECT COUNT(*) FROM songplays;"
user_count = "SELECT COUNT(*) FROM users;"
song_count = "SELECT COUNT(*) FROM songs;"
artist_count = "SELECT COUNT(*) FROM artists;"
time_count = "SELECT COUNT(*) FROM time;"


# ANALYSIS QUERIES
# Q1. What are the top 5 played songs?
Q1 = "What are the top 5 played songs?"
top_songs = ("""
  SELECT s.title, COUNT(*) AS count
  FROM songplays p
  JOIN songs s ON (p.song_id = s.song_id)
  GROUP BY s.title
  ORDER BY count DESC
  LIMIT 5;
""")
# Q2. What is the highest usage time of day by hour for songs?
Q2 = "What are the highest usage hours of a day?"
top_usage_hours = ("""
  SELECT t.hour, SUM(s.duration) AS total_usage
  FROM songplays p
  JOIN songs s ON (p.song_id = s.song_id)
  JOIN time t ON (p.start_time = t.start_time)
  GROUP BY t.hour
  ORDER BY total_usage DESC
  LIMIT 5;
""")

# Q3. Who are the top 5 artists with the longest played songs?
Q3 = "Who are the top 5 artists with the longest played songs?"
top_artists = ("""
  SELECT a.name, SUM(s.duration) as total_duration
  FROM songplays p
  JOIN songs s ON (p.song_id = s.song_id)
  JOIN artists a ON (s.artist_id = a.artist_id)
  GROUP BY a.name
  ORDER BY total_duration DESC
  LIMIT 5;
""")

# Q4. Who are the top 5 users with the most listening records?
Q4 = "Who are the top 5 users with the most listening records?"
top_users = ("""
  SELECT u.user_id, COUNT(*) as total
  FROM songplays p
  JOIN users u ON (p.user_id = u.user_id)
  GROUP BY u.user_id
  ORDER BY total DESC
  LIMIT 5;         
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
counting_queries = [staging_events_count, staging_songs_count, songplay_count, user_count, song_count, artist_count, time_count]
analytic_queries = [top_songs, top_usage_hours, top_artists, top_users]
analytic_questions = [Q1, Q2, Q3, Q4]
tables = ['staging_events', 'staging_songs', 'songplays', 'users', 'songs', 'artists', 'time']
staging_tables = ['staging_events', 'staging_songs']
core_tables = ['songplays', 'users', 'songs', 'artists', 'time']
