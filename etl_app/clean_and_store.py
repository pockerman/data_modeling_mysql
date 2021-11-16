"""
Utilities for cleaning and storing the data
"""

import os
from pathlib import Path
import numpy as np
import glob
import pandas as pd
import db_constants


def process_artist(cursor, datafile, songs_table_insert, artists_table_insert):
    # open song file
    df = pd.read_json(datafile, lines=True)

    # we need to preprocess the column names
    df = df.rename(columns={"artist_name": "name", "artist_location": "location",
                             "artist_latitude":"latitude", "artist_longitude": "longitude"})

    # drop any rows if the artist_id or the song_id are not
    # defined
    df = df[(df['artist_id'].notna()) & (df['song_id'].notna())]

    if df.shape[0] == 0:
        print(datafile)
        print("ERROR: Number of rows after drop NULL {}".format(df.shape[0]))

    else:
        
        # insert artist record
        artist_data = df[['artist_id', 'name', 'location', 'latitude', 'longitude']].values[0].tolist()

        if np.isnan(artist_data[3]):
            artist_data[3] = db_constants.INVALID_LATITUDE

        if np.isnan(artist_data[4]):
            artist_data[4] = db_constants.INVALID_LONGITUDE

        artists_table_insert(cursor=cursor, data=artist_data)

        # insert song record
        song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()

        if np.isnan(song_data[3]):
            artist_data[3] = db_constants.INVALID_YEAR

        if np.isnan(song_data[4]):
            song_data[4] = db_constants.INVALID_SONG_DURATION

        songs_table_insert(cursor, data=song_data)


def collect_files(filepath: Path) -> list:
    """
    Collect all the files in the given filepath
    :param filepath:
    :return: array containing the filepaths
    """

    # tmp holder for the artist files
    result_files = []

    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))

        for f in files:
            result_files.append(os.path.abspath(f))

    return result_files



def process_artists_and_songs(cursor, connection, filepath: Path, songs_table_insert, artists_table_insert):
    """
    Clean and inserts artists
    :param cursor: Cursor to insert data
    :param connection: Connection to the DB
    :param filepath: filepath for artists
    :return:
    """

    artist_files = collect_files(filepath=filepath)

    # get total number of files found
    num_files = len(artist_files)
    print('INFO: {} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(artist_files, 1):

        print("Processing file {0}".format(datafile))
        # procees the artist and the song
        process_artist(cursor, datafile, songs_table_insert=songs_table_insert, artists_table_insert=artists_table_insert)
        connection.commit()
        print('INFO: {}/{} files processed.'.format(i, num_files))


def process_log_file(cursor, filepath,  time_table_insert, user_table_insert):

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        time_table_insert(cursor, data=list(row))
        #cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        user_table_insert(cursor, row)
        #cur.execute(user_table_insert, row)

    """
    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)
    """




def process_logs(cursor, connection, filepath: Path, songs_table_insert,
                                          artists_table_insert):

    files = collect_files(filepath=filepath)

    # get total number of files found
    num_files = len(files)
    print('INFO: {} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(files, 1):
        func(cur, datafile)
        connection.commit()
        print('{}/{} files processed.'.format(i, num_files))



if __name__ == '__main__':

    artists_songs_filepath = Path("/home/alex/qi3/ce_notes_distinct/databases/data_engineer/data_modeling/data/song_data")
    process_artists_and_songs(connection=None, cursor=None, filepath=artists_songs_filepath)

