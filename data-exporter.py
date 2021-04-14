from prometheus_client import start_http_server
import sqlite3
import time
import socket
from functools import reduce

db_connection = sqlite3.connect('traffic.sqlite')
rDNS = {}

def flat_map(f, xs):
    ys = []
    for x in xs:
        ys.extend(f(x))
    return ys

def rdns_lookups(records):
    if not reduce:
        return
    ips = flat_map(lambda x: [x[1], x[2]], records )
    for ip in ips:
        if not ip in rDNS:
            rDNS[ip] = socket.getnameinfo((ip, 0), 0)[0]
            print(ip + "\t" + str(rDNS[ip]))
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
        records = fetch_records(latest_timestamp)
        rdns_lookups(records)
        print(f'Fetched records: {len(records)}')
        if len(records):
            latest_timestamp = update_latest_timestamp(records)
            print(f'Updating latest timestamp to {latest_timestamp}')
        time.sleep(5)


listen()
