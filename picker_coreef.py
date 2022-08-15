import socket
import struct
import json
import time
import os
import sys
import argparse

from datetime import datetime

multicast_group = '239.255.0.42'
server_address = ('', 4242)

# --------------------------------------------------------------------------------
# Class Message
# --------------------------------------------------------------------------------

class Message:

    def __init__(self,message):
        self.device_name = message['device']
        self.sequence = message['sequence']
        self.poll = message['poll']
        self.channel_list = message['channels']
        self.n_channels = len(self.channel_list)
        self.n_samples = len(message['channel_0'])
        self.readings = []
        self.deltas = []
        ts = time.time()
        for s in range(self.n_samples):
            if s > 0:
                delta = message['p_delta_ms'][s-1]
                ts = ts - self.poll - delta/1000.0
                self.deltas.append((self.sequence-s,delta))
            values = []
            for c in range(self.n_channels):
                values.append(message[f'channel_{c}'][s])
            timestamp = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
            reading = (self.sequence-s,ts,timestamp,values)
            if reading[0]>0:
                self.readings.append(reading)

    def __repr__(self):
        class_name = type(self).__name__
        return f'{class_name}(device_name={self.device_name!r},readings={self.readings!r},deltas={self.deltas!r})'

# --------------------------------------------------------------------------------
# Class Device
# --------------------------------------------------------------------------------
class Device:
    def __init__(self,name,address,sequence,poll,channel_list,readings,deltas):
        self.name = name
        self.address = address
        self.last_sequence = sequence
        self.poll = poll
        self.channel_list = channel_list
        self.readings = readings
        self.deltas = deltas
        self.not_written = min(sequence,len(readings))
        self.message_count = 0
        self.last_seen = time.time()
    
    def __init__(self,message,address):
        self.name = message.device_name
        self.address = address
        self.last_sequence = message.sequence
        self.poll = message.poll
        self.channel_list = message.channel_list
        self.readings = message.readings
        self.deltas = message.deltas
        self.not_written = min(message.sequence,len(message.readings))
        self.message_count = 0
        self.last_seen = time.time()

    def n_channels(self):
        return len(self.channel_list)

    def to_json(self):
        content = {
            "name" : self.name,
            "address" : self.address,
            "poll" : self.poll,
            "last_seen": self.last_seen,
            "message_count": self.message_count,
            "channel_list": self.channel_list,
            "readings": self.readings,
            "deltas": self.deltas
        }
        return json.dumps(content,indent=None)

    @staticmethod
    def merge(old_data,new_data):
        result = old_data
        new_entries = 0
        seq_numbers = set()
        for r in result:
            seq_numbers.add(r[0])
        for r in new_data:
            if not r[0] in seq_numbers:
                result.append(r)
                new_entries += 1
        result.sort(key=lambda x: x[0],reverse=True)
        return result, new_entries
    
    def update(self,message):
        self.last_seen=time.time()
        if self.last_sequence == message.sequence:
            # Duplicate or just added, so skip
            pass
        elif self.last_sequence+1 == message.sequence:
            # Message is in sequence, everything is perfect
            self.last_sequence = message.sequence
            self.readings.insert(0,message.readings[0])
            self.deltas.insert(0,message.deltas[0])
            self.message_count += 1
            self.not_written += 1
            print(f'Next sequence number {self.last_sequence} received; reading {message.readings[0]} added.')
        elif self.last_sequence+1 > message.last_sequence:
            print(f'Did not receive sequence numbers {self.last_sequence+1}..{message.sequence}. Trying to recover from backlog.',file=sys.stderr)
            sys.stderr.flush()
            self.last_sequence = message.sequence
            self.message_count += 1
            self.readings, n = merge(self.readings,message.readings)
            self.not_written += n
            self.deltas, _ = merge(self.deltas,message.deltas)
        else:
            # Received sequence number is smaller, assuming device reboot
            print(f'Expecting sequence number {self.last_sequence+1} but received {message.sequence}.',file=sys.stderr)
            sys.stderr.flush()


# --------------------------------------------------------------------------------
# Class Devices
# --------------------------------------------------------------------------------

class Devices:

    def __init__(self,directory_for_data_files,backlog_size,write_frequency):
        self.devices = {}
        self.directory_for_data_files = directory_for_data_files
        self.backlog_size = backlog_size
        self.write_frequency = write_frequency
    
    def write_data_to_file(self,device):
        dt = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        p = os.path.join(self.directory_for_data_files,f"{dt}_{device.name}_data.json")
        with open(p,'w') as fd:
            fd.write(device.to_json())

    def prune(self,l):
        if len(l) > self.backlog_size:
            return l[:self.backlog_size-len(l)]
        return l

    def process_message(self,address,json_message):
        m = Message(json_message)
        print(f'process_message: {m!r}')

        if not m.device_name in self.devices:
            self.devices[m.device_name] = Device(m,address)
        # print(f'New device <{device_name}> added.')
        device = self.devices[m.device_name]
        print(f'device to update: {device!r}')
        device.update(m)
        if device.not_written >= self.write_frequency:
            self.write_data_to_file(device)
            device.not_written = 0
        device.readings = self.prune(device.readings)
        device.deltas = self.prune(device.deltas)
        print(f'updated device: {device!r}')
        print('-----')

# --------------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=str, required=False, help="The directory to store data files",default=".")
    parser.add_argument("--backlog", type=int, required=False, help="The size of the backlog storage",default=24)
    parser.add_argument("--writefreq", type=int, required=False, help="Number of readings before writing to file",default=12)
    parser.add_argument("--details", type=bool, required=False, help="The size of the backlog storage",default=False)

    args = parser.parse_args()

    write_details = args.details
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

    devices = Devices(data_dir,backlog_size,write_frequency)
    while True:
        rawdata, address = sock.recvfrom(1024)
        json_message = json.loads(rawdata)
        devices.process_message(address,json_message)

if __name__ == '__main__':
    main()
