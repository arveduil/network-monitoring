import datetime
import sqlite3
import os
import socket
import time
from typing import Dict, Tuple
from dotenv import load_dotenv
from utils.logs import get_logger
import psycopg2 as pg

from utils.time import get_now

load_dotenv()
log = get_logger('data-exporter')
sqlite = sqlite3.connect('traffic.sqlite')
postgres = pg.connect(
    database=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD'),
    host=os.getenv('POSTGRES_HOST') or '127.0.0.1',
    port=os.getenv('POSTGRES_PORT') or '5432'
)

rDNS = {}

Connection = Tuple[str, str]
persisting_connections: Dict[Connection, datetime.datetime] = {}
CONTINUITY_THRESHOLD = datetime.timedelta(minutes=5)  # TODO: Maybe make configurable, or fine-tune this variable


def create_destination_table():
    cur = postgres.cursor()
    # Semantics for that table are:
    # Upload:   host1 -> host2
    # Download: host1 <- host2
    cur.execute(
        '''CREATE TABLE if not exists Connections (
                        timestamp timestamp,
                        host1 text NOT NULL,
                        host2 text NOT NULL,
                        host1_rdns text,
                        host2_rdns text,
                        download integer NOT NULL default 0,
                        upload integer NOT NULL default 0,
                        duration integer NOT NULL,
                        tags json
        )'''
    )
    postgres.commit()
    cur.execute('''CREATE INDEX if NOT EXISTS host1_index ON Connections(host1)''')
    cur.execute('''CREATE INDEX if NOT EXISTS host2_index ON Connections(host2)''')
    cur.execute('''CREATE INDEX if NOT EXISTS host2_index ON Connections(timestamp)''')
    postgres.commit()


def flat_map(f, xs):
    ys = []
    for x in xs:
        ys.extend(f(x))
    return ys


def rdns_lookups(records):
    ips = flat_map(lambda x: [x[1], x[2]], records)
    for ip in ips:
        if ip not in rDNS:
            rDNS[ip] = socket.getnameinfo((ip, 0), 0)[0]
            log.debug(ip + "\t" + str(rDNS[ip]))


def get_query(timestamp):
    if timestamp:
        return f"SELECT * FROM TrafficLogs WHERE TrafficLogs.timestamp > '{timestamp}'"
    return "SELECT * FROM TrafficLogs"


def fetch_records(timestamp):
    cur = sqlite.cursor()
    cur.execute(get_query(timestamp))
    records = cur.fetchall()

    log.debug(f'Fetched records: {len(records)}')
    return records


def latest_timestamp(records):
    return max([record[0] for record in records])


def clear_outdated():
    # TODO: Implement clearing outdated connections (last_timestamp older than the threshold)
    pass


def insert_connection(connection, timestamp, transfer):
    # It's always upload
    cur = postgres.cursor()
    duration = get_now() - timestamp

    # TODO: Insert tags
    cur.execute('''INSERT INTO Connections (
                                timestamp,
                                host1,
                                host2,
                                host1_rdns,
                                host2_rdns,
                                upload,
                                duration,
                            )
                            VALUES (?, ?, ?, ?, ?)                   
    ''', [timestamp, connection[0], connection[1], rDNS[connection[0]], rDNS[connection[1]], transfer, duration])


def update_connection(connection, duration_delta, upload=None, download=None):
    # TODO: Implement
    cur = postgres.cursor()



# TODO: Implement from pseudocode
def update_connections(new_records):
    for record in new_records:
        timestamp = record[0]
        source, destination = record[1], record[2]
        transfer = record[3]

        connection = (source, destination)
        reversed_connection = (destination, source)

        source_dest = persisting_connections.get(connection)
        dest_source = persisting_connections.get(reversed_connection)
        last_timestamp = source_dest or dest_source  # Essentially means that connection is in cache

        duration_delta = None
        if last_timestamp:
            duration_delta = timestamp - last_timestamp

        if last_timestamp and duration_delta < CONTINUITY_THRESHOLD:
            if source_dest:
                update_connection(connection, duration_delta, upload=transfer)
            elif dest_source:
                update_connection(connection, duration_delta, download=transfer)
        else:
            insert_connection(connection, timestamp, transfer)
        persisting_connections[connection] = timestamp
    clear_outdated()


def process_records(new_records):
    rdns_lookups(new_records)
    update_connections(new_records)


def listen():
    create_destination_table()
    # TODO: It will insert each record gathered from the start every time on restart.
    # TODO: Possible solutions:
    # TODO: 1. Get latest timestamp from PG "Connections" DB, add "duration" seconds, and set as initial timestamp
    # TODO: 2. Implement a safe way of locking the table before fetch and deleting records after successful fetch - then unlocking
    current_timestamp = None
    while True:
        records = fetch_records(current_timestamp)
        process_records(records)

        if len(records):
            current_timestamp = latest_timestamp(records)
        time.sleep(5)


listen()
