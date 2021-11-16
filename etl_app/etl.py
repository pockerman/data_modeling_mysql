import os
from pathlib import Path
import json
from etl_app.create_db import create_db
from mysql_queries import insert_into_songs, insert_into_artists
from etl_app.clean_and_store import process_artists_and_songs, process_logs


def main(config_file: Path) -> None:
    """
    Runs the the ETL pipeline
    :return:
    """

    # read the configuration
    with open(config_file, "r") as cf:

        print("INFO: Loading configuration file at {0}".format(config_file))
        configuration = json.load(cf)

        db_config_file = configuration["db_config_file"]

        with open(db_config_file, "r") as df:
            print("INFO: Loading DB configuration file at {0}".format(db_config_file))
            db_configuration = json.load(df)
            try:
                print("INFO: Loading configuration file at {0}".format(config_file))
                print("INFO: Creating DB....")
                connection, cursor = create_db(config=db_configuration)
                print("INFO: Done creating DB....")
                print("INFO: Create artists and songs tables....")

                # read data and clean them
                # we could write into a separate
                # file so that another process writes the cleaned data
                # into the DB but we do it all in one go instead
                # read and clean data
                process_artists_and_songs(cursor=cursor, connection=connection,
                                          filepath=configuration['song_data_file'],
                                          songs_table_insert=insert_into_songs,
                                          artists_table_insert=insert_into_artists)

                print("INFO: Create logs tables....")
                process_logs(cursor=cursor, connection=connection,
                                          filepath=configuration['log_data_file'],
                                          songs_table_insert=insert_into_songs,
                                          artists_table_insert=insert_into_artists)
            except Exception as e:
                print("ERROR: Could not create DB. Error msg: {0}".format(str(e)))



if __name__ == '__main__':

    """
    Starting point for the ETL
    """
    config_file = Path('config_file.json')
    main(config_file=config_file)
