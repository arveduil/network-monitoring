import socket
import binascii
import struct
import os
import sys
import sqlite3
from enum import Enum
from typing import Optional

from utils.logs import get_logger
from utils.time import get_now

log = get_logger('network-sniffer')


class EthernetProtocol(Enum):
    IPV4 = "IPV4"
    IPV6 = "IPV6"


def get_proto(hex_proto_type) -> Optional[EthernetProtocol]:
    if hex_proto_type == '0x800':
        return EthernetProtocol.IPV4
    elif hex_proto_type == '0x86dd':
        return EthernetProtocol.IPV6


def ethernet_frame(data):
    header = struct.unpack("!6s6sH", data[0:14])

    dst_mac = binascii.hexlify(header[0])
    src_mac = binascii.hexlify(header[1])
    proto_type = header[2]

    proto = get_proto(hex(proto_type))

    payload = data[14:]

    return dst_mac, src_mac, proto, payload


def get_connection(data):
    _, _, src, target = struct.unpack('! 8x B B 2x 4s 4s', data[:20])
    return ipv4(src), ipv4(target)


def ipv4(addr):
    return '.'.join(map(str, addr))


# TODO: Consider inserting them in bigger intervals, which would make the amount smaller
def create_table_with_index(db):
    cur = db.cursor()
    cur.execute('''CREATE TABLE if not exists TrafficLogs 
                      (timestamp datetime, source text, destination text, data integer)''')
    cur.execute('''CREATE INDEX if not exists timestamp_index ON TrafficLogs(timestamp)''')
    db.commit()


def monitor(db):
    raw_socket = socket.socket(socket.PF_PACKET, socket.SOCK_RAW, socket.ntohs(3))
    connections = {}

    last_insert_timestamp_seconds = None
    while True:
        now_seconds = get_now().second
        if now_seconds != last_insert_timestamp_seconds and now_seconds % 5 == 0:
            last_insert_timestamp_seconds = now_seconds

            timestamp = get_now()
            cursor = db.cursor()
            cursor.executemany(f"INSERT INTO TrafficLogs VALUES (?, ?, ?, ?)", [
                [timestamp, connection[0], connection[1], connections[connection]]
                for connection in connections
            ])
            db.commit()

            log.debug(f'SQL Executed: INSERT INTO TrafficLogs ({len(connections.keys())} records) ')
            connections = {}

        raw_data, addr = raw_socket.recvfrom(65536)
        dest_mac, src_mac, eth_proto, data = ethernet_frame(raw_data)

        if eth_proto == EthernetProtocol.IPV4:
            connection = get_connection(data)
        else:
            # TODO handle IPv6 and other
            continue

        connections[connection] = (connections.get(connection) or 0) + len(raw_data)


if __name__ == '__main__':
    db_connection = None
    try:
        db_connection = sqlite3.connect('traffic.sqlite')
        create_table_with_index(db_connection)
        monitor(db_connection)
        db_connection.close()
    except KeyboardInterrupt:
        log.error('Program received interrupt signal!')
        if db_connection:
            db_connection.close()
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
