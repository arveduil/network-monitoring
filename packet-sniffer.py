#! /usr/local/bin/python3.5

import socket
import binascii
import struct
import sys
import os
import datetime
import sqlite3


def get_now():
    return datetime.datetime.now()


def create_insert_sql_query(timestamp: datetime, src: str, dest: str, size: int):
    return str(f"""INSERT INTO TrafficLogs VALUES ('{timestamp}', '{src}', '{dest}', {str(size)})  """)


def main( db_connection):
    conn = socket.socket(socket.PF_PACKET, socket.SOCK_RAW, socket.ntohs(3))
    connections = {}
    packet_count = 0
    cur = db_connection.cursor()

    cur.execute('''CREATE TABLE if not exists TrafficLogs 
                   (timestamp datetime, source text, destination text, data integer)''')
    db_connection.commit()

    last_insert_timestamp_seconds = -1
    while True:
        now_seconds =get_now().second
        if now_seconds != last_insert_timestamp_seconds and now_seconds % 5 == 0:
            last_insert_timestamp_seconds = now_seconds
            for connection in connections:
                inesrt_transfer_log(connection, connections[connection], cur)

            connections = {}
            db_connection.commit()

        packet_count = packet_count + 1
        raw_data, addr = conn.recvfrom(65536)
        dest_mac, src_mac, eth_proto, data = ethernet_frame(raw_data)
        data_size = len(raw_data)
        if eth_proto == 'IPV4':
            srcDest = get_source_destination(data, raw_data)
        else:
            # TODO handle IPv6 and other
            continue
        if srcDest in connections:
            connections[srcDest] = connections[srcDest] + data_size
        else:
            connections[srcDest] = data_size
    db_connection.close()


def inesrt_transfer_log(connection, connection_transfer, cur):
    insert_query = create_insert_sql_query(get_now(), connection[0], connection[1], connection_transfer)
    print(insert_query)
    cur.execute(insert_query)


def get_source_destination(data, raw_data):
    (version, header_length, ttl, proto, src, target, data) = ipv4_Packet(data)
    return (src, target)


def ethernet_frame(data):
    # print('ETH FRAME:' + str(len(data)))
    proto = ""
    IpHeader = struct.unpack("!6s6sH", data[0:14])
    dstMac = binascii.hexlify(IpHeader[0])
    srcMac = binascii.hexlify(IpHeader[1])
    protoType = IpHeader[2]
    nextProto = hex(protoType)

    if (nextProto == '0x800'):
        proto = 'IPV4'
    elif (nextProto == '0x86dd'):
        proto = 'IPV6'

    data = data[14:]

    return dstMac, srcMac, proto, data


def ipv4_Packet(data):
    version_header_len = data[0]
    version = version_header_len >> 4
    header_len = (version_header_len & 15) * 4
    ttl, proto, src, target = struct.unpack('! 8x B B 2x 4s 4s', data[:20])
    return version, header_len, ttl, proto, ipv4(src), ipv4(target), data[header_len:]


def ipv4(addr):
    return '.'.join(map(str, addr))


def ipv6Header(data):
    ipv6_src_ip = socket.inet_ntop(socket.AF_INET6, data[8:24])
    ipv6_dst_ip = socket.inet_ntop(socket.AF_INET6, data[24:40])
    return (ipv6_src_ip, ipv6_dst_ip)


try:
    db_connection = sqlite3.connect('traffic.sqlite')
    main( db_connection)
except KeyboardInterrupt:
    print('INTERUPTED !!!')
    db_connection.close()
    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)
