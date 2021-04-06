#! /usr/local/bin/python3.5

import socket
import struct
import textwrap
import binascii
import struct
import sys
import os


def main():
    conn = socket.socket(socket.PF_PACKET, socket.SOCK_RAW, socket.ntohs(3))

    connections = {}
    rdns = {}

    while True:
        raw_data, addr = conn.recvfrom(65536)
        dest_mac, src_mac, eth_proto, data = ethernet_frame(raw_data)
        data_size = len(raw_data)
        if eth_proto == 'IPV4':
            srcDest = (src, dest) = getSrcDest(data, raw_data)
        else:
            srcDest = (src, dest) = ipv6Header(data)
        rdns[src] = socket.getnameinfo((src, 0), 0)
        rdns[dest] = socket.getnameinfo((dest, 0), 0)
        if srcDest in connections:
            connections[srcDest] = connections[srcDest] + data_size
        else:
            connections[srcDest] = data_size
        clear = lambda: os.system('clear')
        clear()
        for connection in connections:
            connection_transfer = connections[connection]
            print(str(connection) + '\t' + str(connection_transfer) + ' B' + '\t' + '{0:.0f}'.format(
                connection_transfer / 1024) + ' KB' + '\t' + '{0:.0f}'.format(connection_transfer / 1024 / 1024) + ' MB' + '\t' + str(
                rdns[connection[0]]) + ' -> ' + str(rdns[connection[1]]))



def getSrcDest(data, raw_data):
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
    return (ipv6_src_ip, ipv6_dst_ip )


main()
