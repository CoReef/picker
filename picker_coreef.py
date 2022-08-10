import socket
import struct
import json
import time
import os
import argparse

from datetime import datetime

multicast_group = '239.255.0.42'
server_address = ('', 4242)
backlog_size = 24
write_frequency = 12

devices = {}

def new_device (d,address):
    initial = {
        "name":d['device'],
        "address":address,
        "last_seq":d['sequence']-1,
        "r_timebase":d['current'],
        "l_timebase":time.time(),
        "last_seen":time.time(),
        "mcount":0,
        "readings":[],
        "not_written":0
        }
    return initial

def write_data_to_file(dir,d):
    dt = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    p = os.path.join(dir,f"{dt}_{d['name']}_data.json")
    with open(p,'w') as fd:
        fd.write(json.dumps(d,indent=4))

def process_message (d,message):
    d['last_seen']=time.time()
    if d['last_seq'] == message['sequence']:
        return
    if message['sequence'] == d['last_seq']+1:
        d['last_seq'] += 1
        d['mcount'] += 1
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        reading = (message['sequence'],time.time(),timestamp,message['current_t'],message['current_h'])
        d['readings'].append(reading)
        d['not_written'] += 1
        print(d)
    elif message['sequence'] > d['last_seq']+1:
        print(f'Did not receive sequence numbers {d["last_seq"]+1}..{message["sequence"]}. Trying to recover from backlog')
        d['last_seq'] = message['sequence']
        d['mcount'] += 1
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        reading = (message['sequence'],time.time(),timestamp,message['current_t'],message['current_h'])
        d['readings'].append(reading)
        d['not_written'] += 1
        print(d)
    else:
        print(f'Expecting sequence number <{d["last_seq"]+1}> but received <{message["sequence"]}>')


def main():
    global write_frequency
    if backlog_size < write_frequency:
        write_frequency = backlog_size

    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=str, required=False, help="The directory to store data files",default=".")
    args = parser.parse_args()

    # Check for the output directory and create it if not
    data_dir = os.path.abspath(args.outdir)
    if not os.path.isdir(data_dir):
        os.makedirs(data_dir)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(server_address)
    group = socket.inet_aton(multicast_group)
    mreq = struct.pack('4sL', group, socket.INADDR_ANY)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    while True:
        rawdata, address = sock.recvfrom(1024)
        data = json.loads(rawdata)
        device_name = data['device']
        if not device_name in devices:
            devices[device_name] = new_device(data,address)
        process_message(devices[device_name],data)
        if devices[device_name]['not_written'] == write_frequency:
            write_data_to_file(data_dir,devices[device_name])
            devices[device_name]['not_written'] = 0
        if len(devices[device_name]['readings']) > backlog_size:
            devices[device_name]['readings'] = devices[device_name]['readings'][1:]

if __name__ == '__main__':
    main()
