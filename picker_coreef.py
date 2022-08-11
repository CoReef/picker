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

def new_device (device_name,address,sequence,sec_since_boot,channel_list,readings):
    initial = {
        "name":device_name,
        "address":address,
        "last_seq":sequence,
        "r_timebase":sec_since_boot,
        "l_timebase":time.time(),
        "last_seen":time.time(),
        "mcount":0,
        "channels":channel_list,
        "n_channels":len(channel_list),
        "readings":readings,
        "not_written":min(sequence,len(readings))
        }
    return initial

def write_data_to_file(dir,d):
    dt = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    p = os.path.join(dir,f"{dt}_{d['name']}_data.json")
    with open(p,'w') as fd:
        fd.write(json.dumps(d,indent=4))

def disect_message(message):
    device_name = message['device']
    sequence = message['sequence']
    sec_since_boot = message['sec_since_boot']
    channel_list = message['channels']
    n_channels = len(channel_list)
    n_samples = len(message['channel_0'])
    readings = []
    for s in range(n_samples):
        values = []
        for c in range(n_channels):
            values.append(message[f'channel_{c}'][s])
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        reading = (sequence-s,time.time(),timestamp,values)
        if reading[0]>0:
            readings.append(reading)
    return device_name,sequence,sec_since_boot,channel_list,readings

def merge_readings(device,new_readings):
    result = device['readings']
    seq_numbers = set()
    for r in result:
        seq_numbers.add(r[0])
    for r in new_readings:
        if not r[0] in seq_numbers:
            result.append(r)
            device['not_written'] += 1
    result.sort(key=lambda x: x[0],reverse=True)
    device['readings'] = result

def process_message (d,address,message,data_dir):
    device_name,sequence,sec_since_boot,channel_list,readings = disect_message(message)
    if not device_name in devices:
        devices[device_name] = new_device(device_name,address,sequence,sec_since_boot,channel_list,readings)
        print(f'New device <{device_name}> added.')
    device = devices[device_name]
    device['last_seen']=time.time()
    if device['last_seq']==sequence:
        # Duplicate or just added, so skip
        pass
    elif sequence == device['last_seq']+1:
        # Message is in sequence, everything is perfect
        device['last_seq']=sequence
        device['readings'].insert(0,readings[0])
        device['mcount'] += 1
        device['not_written'] += 1
        print(f'Next sequence number {sequence} received; reading {readings[0]} added.')
    elif sequence > device['last_seq']+1:
        print(f'Did not receive sequence numbers {device["last_seq"]+1}..{sequence}. Trying to recover from backlog.')
        device['last_seq'] = sequence
        device['mcount'] += 1
        merge_readings(device,readings)
    else:
        # Received sequence number is smaller, assuming device reboot
        print(f'Expecting sequence number {device["last_seq"]+1} but received {sequence}.')

    if device['not_written'] >= write_frequency:
        write_data_to_file(data_dir,device)
        device['not_written'] = 0

    if len(device['readings']) > backlog_size:
        device['readings'] = device['readings'][:backlog_size-len(device['readings'])]
 

def main():
    global write_frequency
    global backlog_size

    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=str, required=False, help="The directory to store data files",default=".")
    parser.add_argument("--backlog", type=int, required=False, help="The size of the backlog storage",default=24)
    parser.add_argument("--writefreq", type=int, required=False, help="Number of readings before writing to file",default=12)
    args = parser.parse_args()

    backlog_size = args.backlog
    write_frequency = args.writefreq
    if backlog_size < write_frequency:
        write_frequency = backlog_size

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
        json_message = json.loads(rawdata)
        process_message(devices,address,json_message,data_dir)

if __name__ == '__main__':
    main()
