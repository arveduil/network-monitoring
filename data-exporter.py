import datetime
import sqlite3
import os
import socket
import time
from typing import Dict, Tuple, Optional
from dotenv import load_dotenv
import json
from mappings import extract_tag
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
                        duration double precision NOT NULL,
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


def timedelta_to_psql(timedelta: datetime.timedelta):
    return format(timedelta.total_seconds(), 'f')


def rdns_lookups(records):
    ips = flat_map(lambda x: [x[1], x[2]], records)
    for ip in ips:
        if ip not in rDNS:
            try:
                rDNS_result = socket.getnameinfo((ip, 0), 0)[0]
                if rDNS_result is None:
                    rDNS_result = ip
                rDNS[ip] = rDNS_result
                log.debug(ip + "\t" + str(rDNS[ip]))
            except:
                log.debug("Error during rDNS with ip: " + ip)


def fetch_records(timestamp):
    cur = sqlite.cursor()
    query = "SELECT * FROM TrafficLogs"
    if timestamp:
        query = f"SELECT * FROM TrafficLogs WHERE TrafficLogs.timestamp > '{timestamp}'"

    cur.execute(query)
    records = cur.fetchall()

    log.debug(f'Fetched records: {len(records)}')
    return records


def latest_timestamp(records):
    return max([record[0] for record in records])


def clear_outdated():
    # TODO: Implement clearing outdated connections (last_timestamp older than the threshold)
    pass


def get_connection_tags(connection):
    tag_table = []

    tag_1 = extract_tag(connection[0])
    if tag_1:
        tag_table.append(tag_1)
    tag_2 = extract_tag(connection[1])
    if tag_2:
        tag_table.append(tag_2)

    return tag_table


def insert_connection(connection, timestamp: datetime, transfer):
    # It's always upload
    cur = postgres.cursor()
    duration = get_now() - timestamp

    cur.execute("""
        INSERT INTO Connections VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
    """, [
        timestamp,
        connection[0],
        connection[1],
        rDNS.get(connection[0]),
        rDNS.get(connection[1]),
        0,
        transfer,
        duration.total_seconds(),
        json.dumps(get_connection_tags(connection))
    ])


def update_connection(connection, duration_delta, upload=None, download=None):
    # TODO: Implement
    cur = postgres.cursor()
    if not upload and not download:
        raise Exception('No upload or download for connection update')

    query = ""
    delta_seconds = timedelta_to_psql(duration_delta)
    if upload:
        query = f"""
            UPDATE Connections 
            SET upload = upload + {upload},
                duration = duration + {delta_seconds}
            WHERE host1='{connection[0]}' AND host2='{connection[1]}'
            AND timestamp = (
                SELECT timestamp from Connections
                WHERE host1='{connection[0]}' AND host2='{connection[1]}'
                ORDER BY timestamp DESC
                LIMIT 1
            )
        """
    if download:
        query = f"""
            UPDATE Connections 
            SET download = download + {download},
                duration = duration + {delta_seconds}
            WHERE host1='{connection[0]}' AND host2='{connection[1]}'
            AND timestamp = (
                SELECT timestamp from Connections
                WHERE host1='{connection[0]}' AND host2='{connection[1]}'
                ORDER BY timestamp DESC
                LIMIT 1
            )       
        """

    cur.execute(query)


def update_connections(new_records):
    for record in new_records:
        timestamp = datetime.datetime.fromisoformat(record[0])
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
    postgres.commit()


def get_current_timestamp() -> Optional[datetime.datetime]:
    cur = postgres.cursor()
    cur.execute("""
        SELECT "timestamp" from Connections
        ORDER BY "timestamp" DESC
        LIMIT 1 
    """)
    result = cur.fetchall()
    if len(result) > 0:
        return result[0][0]
    return None


def listen():
    create_destination_table()
    # TODO: Implement a safe way of locking the table before fetch and deleting records after successful fetch - then unlocking
    current_timestamp = get_current_timestamp()
    while True:
        records = fetch_records(current_timestamp)
        process_records(records)

        if len(records):
            current_timestamp = latest_timestamp(records)
        time.sleep(5)


listen()
