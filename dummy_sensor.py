import socket
import struct
import json
import time
import random
import argparse

from datetime import datetime

multicast_group = '239.255.0.42'
multicast_port = 4242
multicast_ttl = 1

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", type=str, required=False, help="The name of this sensor device",default="Dummy")
    parser.add_argument("--channels", type=int, required=False, help="The number of channels",default=1)
    parser.add_argument("--readings", type=int, required=False, help="Number of readings in message",default=10)
    parser.add_argument("--sleep", type=int, required=False, help="Seconds between two messages",default=10)
    parser.add_argument("--droprate", type=int, required=False, help="Drop probability [0,100) ",default=0)
    args = parser.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack('b',multicast_ttl))

    start = time.time()

    n_channels = args.channels
    n_readings = args.readings
    drop_rate = args.droprate

    message = {
        "device":args.name,
        "sequence":0,
        "sec_since_boot":int(time.time()-start),
        "channels":[]
    }
    for c in range(n_channels):
        message["channels"].append(f' Channel_{c}')
        message[f'channel_{c}'] = []
        for r in range(n_readings):
            message[f'channel_{c}'].append(-100.0)

    while True:
        message["sequence"] += 1
        message["sec_since_boot"] = int(time.time()-start)
        for c in range(n_channels):
            message[f'channel_{c}'] = [message["sequence"]] + message[f'channel_{c}'][:-1]

        json_message = json.dumps(message,indent=4)
        raw_message = bytes(json_message,"UTF-8")

        if random.randint(0,99) >= drop_rate:
            print(f'Send message {message["sequence"]}')
            sent = sock.sendto(raw_message,(multicast_group,multicast_port))
        else:
            print(f'Message {message["sequence"]} dropped')

        time.sleep(args.sleep)

if __name__ == '__main__':
    main()
