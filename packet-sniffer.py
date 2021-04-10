import pyshark
import socket
import textwrap
import binascii
import struct
import sys
import os

capture = pyshark.LiveCapture(interface='eth0')
connections = {}
rdns = {}


for packet in capture.sniff_continuously(packet_count=100):
    if 'IP' in packet:
        data_size = int(packet.length)
        srcDest = (src, dest) = (packet['ip'].src, packet['ip'].dst)
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
            print(str(connection) + '\t' + str(connection_transfer) + ' B'  +  '\t'  +
                  '{0:.0f}'.format(
                connection_transfer / 1024) + ' KB' + '\t' + '{0:.0f}'.format(
                connection_transfer / 1024 / 1024) + ' MB' + '\t'
                  +
                  str(
                rdns[connection[0]]) + ' -> ' + str(rdns[connection[1]]))
