import mysql.connector


ARTISTS_TABLE = 'artists'
SONGS_TABLE = 'songs'
USERS_TABLE = 'users'
TIME_TABLE = 'time'

SONGS_TABLE_INSERT = 'INSERT INTO songs (song_id, title, artist_id, year, duration) ' \
                     'VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE song_id = song_id'
ARTISTS_TABLE_INSERT = 'INSERT INTO artists (artist_id, name, location, latitude, longitude) ' \
                       'VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE artist_id = artist_id'

TIME_TABLE_INSERT = ' INSERT INTO time (start_time, hour, day, week, month, year, weekday) ' \
                    'VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE start_time = start_time'

USER_TABLE_INSERT = 'INSERT INTO users (user_id, first_name, last_name, gender, level) ' \
                    'VALUES (%s, %s, %s, %s, %s) ' \
                    'ON DUPLICATE KEY UPDATE ' \
                    'user_id = user_id, first_name = first_name, last_name = last_name, gender = gender, level = level'


def connect(host, port, user, password):
    mydb = mysql.connector.connect(
        host=host,
        user=user,
        password=password
    )

    return mydb


def create_db(cursor, db_name):
    """
    Create the database with the given name
    :param cursor: Cursor to the DB
    :param db_name: db name to create
    :return: None
    """
    sql_str = "DROP DATABASE IF EXISTS {0};".format(db_name)
    cursor.execute(sql_str)
    sql_str = "CREATE DATABASE {0};".format(db_name)
    cursor.execute(sql_str)

    sql_str = "SHOW DATABASES LIKE '{0}'".format(db_name)
    cursor.execute(sql_str)

    result = cursor.fetchone()

    if db_name in result:
        return True

    return False


def create_tables(cursor) -> None:

    # artists table
    sql_str = "CREATE TABLE artists (artist_id VARCHAR(255) PRIMARY KEY, name VARCHAR(255), location TEXT, " \
              "latitude FLOAT, longitude FLOAT)"
    cursor.execute(sql_str)

    # songs table
    sql_str = "CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR(255) PRIMARY KEY, title VARCHAR(255), " \
              "artist_id VARCHAR(255) REFERENCES artists (artist_id), year INTEGER, duration FLOAT)"
    cursor.execute(sql_str)

    # users table
    sql_str = "CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY,  " \
              "first_name VARCHAR(50),  last_name VARCHAR(50),  gender CHAR(1),  level VARCHAR(255))"
    cursor.execute(sql_str)

    sql_str = 'CREATE TABLE IF NOT EXISTS time ( start_time TIMESTAMP PRIMARY KEY, ' \
              'hour INTEGER,  day INTEGER,  week INTEGER,  month INTEGER,  year INTEGER,  weekday VARCHAR(50))'
    cursor.execute(sql_str)

    sql_str = 'CREATE TABLE IF NOT EXISTS songplays ( songplay_id SERIAL PRIMARY KEY,  ' \
              'start_time TIMESTAMP REFERENCES time (start_time),  ' \
              'user_id INTEGER REFERENCES users (user_id),  ' \
              'level VARCHAR(255),  ' \
              'song_id VARCHAR(255) REFERENCES songs (song_id),  ' \
              'artist_id VARCHAR(255) REFERENCES artists (artist_id),  ' \
              'session_id INTEGER,  location TEXT, user_agent TEXT)'
    cursor.execute(sql_str)


def insert_into_songs(cursor, data):
    cursor.execute(SONGS_TABLE_INSERT, data)


def insert_into_artists(cursor, data):
    cursor.execute(ARTISTS_TABLE_INSERT, data)


def time_table_insert(cursor, data):
    cursor.execute(TIME_TABLE_INSERT, data)


def user_table_insert(cursor, data):
    cursor.execute(USER_TABLE_INSERT, data)






