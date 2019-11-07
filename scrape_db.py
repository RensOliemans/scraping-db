import csv
import os
import sys

import psycopg2
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DB_NAME = os.getenv("DB_NAME")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")

FILENAME = 'wifiexport.csv'
TIME_START = '2018-09-01'
TIME_END = '2018-09-02'


@logger.catch
def connect_to_db(host, database, port, user, password):
    logger.debug("Connecting to database")
    connect = psycopg2.connect(host=host, database=database, port=port,
                               user=user, password=password)
    logger.debug("Connected to database")

    return connect


@logger.catch
def perform_query(cursor, time_start, time_end):
    sql_query = f"SELECT * \
                  FROM wifidata.association_counts \
                  WHERE time >= '{time_start}' \
                       AND time <= '{time_start}'"

    logger.debug("Fetching results")
    cursor.execute(sql_query)

    headers = [i[0] for i in cursor.description]
    results = cursor.fetchall()
    logger.debug("Obtained results")

    return headers, results


@logger.catch
def write_to_csv(filename, headers, results):
    logger.debug("Opening csv file")
    csv_file = csv.writer(open(filename, 'w', newline=''),
                          delimiter=',', lineterminator='\n')

    logger.debug("Writing headers")
    csv_file.writerow(headers)
    logger.debug("Writing rows")
    csv_file.writerows(results)

    logger.debug("Writing successful")


def setup_db_connection():
    connect = connect_to_db(HOST, DB_NAME, PORT, USER, PASSWORD)
    cursor = connect.cursor()

    return connect, cursor

def import_data(connection):
    _, cursor = connection
    headers, results = perform_query(cursor, TIME_START, TIME_END)
    return (headers, results)

def export_data(data):
    headers, results = data
    write_to_csv(FILENAME, headers, results)

def cleanup_db_connection(connection):
    connect, _ = connection
    connect.close()


def main():
    connection = setup_db_connection()

    data = import_data(connection)
    export_data(data)

    cleanup_db_connection(connection)

main()
