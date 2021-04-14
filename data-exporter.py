from prometheus_client import start_http_server
import sqlite3
import time

db_connection = sqlite3.connect('traffic.sqlite')


def rdns_lookups():
    # TODO: Implement, preferably with a table which caches those
    return


def get_query(latest_timestamp):
    if latest_timestamp:
        return f"SELECT * FROM TrafficLogs WHERE TrafficLogs.timestamp > '{latest_timestamp}'"
    return "SELECT * FROM TrafficLogs"


def fetch_records(latest_timestamp):
    cur = db_connection.cursor()
    cur.execute(get_query(latest_timestamp))
    return cur.fetchall()


def update_latest_timestamp(records):
    max_timestamp = max([record[0] for record in records])
    return max_timestamp


def listen():
    start_http_server(8000)

    latest_timestamp = None
    while True:
        rdns_lookups()
        records = fetch_records(latest_timestamp)
        print(f'Fetched records: {len(records)}')
        if len(records):
            latest_timestamp = update_latest_timestamp(records)
            print(f'Updating latest timestamp to {latest_timestamp}')
        time.sleep(5)


listen()
