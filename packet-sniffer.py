import socket
import binascii
import struct
import os
import sys
import datetime
import sqlite3
import logging
from enum import Enum
from typing import Union


def get_now():
    return datetime.datetime.now()


def configure_logging():
    logger = logging.getLogger('network-sniffer')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    launch_time = str(get_now()).replace(' ', '_')
    f_handler = logging.FileHandler(f'./logs/packet-sniffer-{launch_time}.log')
    c_handler = logging.StreamHandler()

    c_handler.setLevel(logging.DEBUG)
    f_handler.setLevel(logging.DEBUG)
    c_handler.setFormatter(formatter)
    f_handler.setFormatter(formatter)
    logger.addHandler(f_handler)
    logger.addHandler(c_handler)
    return logger


log = configure_logging()


class EthernetProtocol(Enum):
    IPV4 = "IPV4"
    IPV6 = "IPV6"


def insert_sql_query(timestamp: datetime, src: str, dest: str, size: int):
    return f"INSERT INTO TrafficLogs VALUES ('{timestamp}', '{src}', '{dest}', {str(size)})"


def insert_transfer_log(connection, connection_transfer, cursor):
    insert_query = insert_sql_query(
        timestamp=get_now(),
        src=connection[0],
        dest=connection[1],
        size=connection_transfer
    )
    log.debug(f'Executing SQL: {insert_query}')
    cursor.execute(insert_query)


def get_proto(hex_proto_type) -> Union[EthernetProtocol, None]:
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


def create_table(db):
    cur = db.cursor()
    cur.execute('''CREATE TABLE if not exists TrafficLogs 
                      (timestamp datetime, source text, destination text, data integer)''')
    db.commit()


def monitor(db):
    raw_socket = socket.socket(socket.PF_PACKET, socket.SOCK_RAW, socket.ntohs(3))
    connections = {}
    packet_count = 0

    last_insert_timestamp_seconds = None
    while True:
        now_seconds = get_now().second
        if now_seconds != last_insert_timestamp_seconds and now_seconds % 5 == 0:
            last_insert_timestamp_seconds = now_seconds
            for connection in connections:
                transfer = connections[connection]
                insert_transfer_log(connection, transfer, db.cursor())

            connections = {}
            db.commit()

        packet_count = packet_count + 1

        raw_data, addr = raw_socket.recvfrom(65536)
        dest_mac, src_mac, eth_proto, data = ethernet_frame(raw_data)

        if eth_proto == EthernetProtocol.IPV4:
            connection = get_connection(data)
        else:
            # TODO handle IPv6 and other
            continue

        connections[connection] = (connections.get(connection) or 0) + len(raw_data)

    db.close()


db_connection = None

try:
    db_connection = sqlite3.connect('traffic.sqlite')
    create_table(db_connection)
    monitor(db_connection)
except KeyboardInterrupt:
    log.error('Program received interrupt signal!')
    if db_connection:
        db_connection.close()
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)
