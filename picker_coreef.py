import socket
import struct
import json

multicast_group = '239.255.0.42'

server_address = ('', 4242)

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(server_address)
    group = socket.inet_aton(multicast_group)
    mreq = struct.pack('4sL', group, socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    while True:
        print(f"Waiting for UDP multicast message <{multicast_group}> ...")
        rawdata, address = sock.recvfrom(1024)
        print(f"\nReceived {len(rawdata)} bytes from {address}\n")
        print(f"Content is <{rawdata}>\n")
        data = json.loads(rawdata)
        print(f"as dict: <{data}>\n")
        print(f"The current temperature is {data['temperature']} Celsius\n")

if __name__ == '__main__':
    main()
