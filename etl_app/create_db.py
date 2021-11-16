"""
Create MySQL or any other DB type passed
at run time via the db_config file
"""
import json


def create_mysql(configuration):
    """
    Create the MySQL database
    :return:
    """

    from mysql_queries import create_db, connect, create_tables, insert_into_songs, insert_into_artists

    connection = connect(host="localhost", port=3306, user='root', password='david_hilbert')
    cursor = connection.cursor()

    result = create_db(cursor=cursor, db_name='sparkify')

    if result is False:
        raise Exception("Database was not created...")

    # make sure we are using the DB
    cursor.execute("USE {0}".format("sparkify"))

    # now that we have created the DB
    # we will create the tables
    create_tables(cursor=cursor)

    return connection, cursor


def create_db(config):
    """
    Create the DB according to the configuration given
    :param config:
    :return:
    """

    print("INFO: Creating DB {0}".format(config["db_name"]))
    print("INFO: RDBMS {0}".format(config["db_type"]))
    print("INFO: RDBMS host:port {0}:{1}".format(config["db_host"], config["db_port"]))

    if config["db_type"] == "MySQL":
        return create_mysql(configuration=config)


if __name__ == '__main__':

    config_file = "db_config.json"
    with open(config_file, "r") as f:
        configuration = json.load(f)
        create_db(config=configuration)
