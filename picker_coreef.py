import socket
import struct
import json
import time
import os
import sys
import argparse

from datetime import datetime
from collections import namedtuple

multicast_group = '239.255.0.42'
server_address = ('', 4242)

# --------------------------------------------------------------------------------
# Message
# --------------------------------------------------------------------------------

Reading = namedtuple('Reading', 'sequence r_timebase r_delta values')
Header = namedtuple('Header', 'device_name poll_frequency free_heap n_channels channel_list n_readings sequence_number')
Message = namedtuple('Message', 'header readings l_timebase r_timebase')


def as_message(raw_message, time_received):
    """Convert the raw JSON formatted message received at the given time to a dictionary"""
    j = json.loads(raw_message)
    # Build the message header
    header = Header(j['device'], j['poll'], j.get('free_heap', 0), len(j['channels']), j['channels'], len(j['channel_0']),j['sequence'])
    # Collect all readings as a dictionary with sequence number as the key
    readings = {}
    tb = j['timebase']
    for i in range(header.n_readings):
        seq_number = j['sequence'] - i
        values = tuple(j[f'channel_{c}'][i] for c in range(header.n_channels))
        delta = 0 if i == 0 else j['t_deltas'][i - 1]
        r = Reading(seq_number, tb, delta, values)
        tb = tb - delta - header.poll_frequency * 1000.0
        if seq_number >= 0:
            readings[seq_number] = r # Add all readings with sequence number > 0 to dictionary
    m = Message(header,readings,time_received,j['timebase'])
    return m

# --------------------------------------------------------------------------------
# Class Device
# --------------------------------------------------------------------------------

class Device:
    """Device keeps track of all state information and limited number of readings"""
    def __init__(self, message, address):
        """Most state information for a new device is part of any message"""
        self.name = message.header.device_name
        self.address = address
        self.last_sequence = message.header.sequence_number
        self.poll_frequency = message.header.poll_frequency
        self.channel_list = message.header.channel_list
        self.not_written = min(self.last_sequence, len(message.readings))
        self.message_count = 1
        self.last_seen = time.time()
        self.first_seen_r = message.r_timebase
        self.first_seen_l = message.l_timebase
        self.free_heap = message.header.free_heap
        self.readings = message.readings
        print(f'Have readings for the following sequence numbers {self.key_list()}')

    def __repr__(self):
        class_name = type(self).__name__
        # {k: v for k, v in a.__class__.__dict__.items() if not k.startswith('__')}?
        s = self.readings.keys()
        return (
            f'{class_name}({self.name!r},address={self.address!r},last_sequence={self.last_sequence!r},'
            f'readings={self.readings[max(s)]!r}...{self.readings[min(s)]!r},'
            f'free_heap={self.free_heap})'
        )

    def to_json(self):
        """A subset of the device state is converted to a JSON string"""
        content = {
            "name": self.name,
            "address": self.address,
            "poll": self.poll_frequency,
            "last_seen": self.last_seen,
            "message_count": self.message_count,
            "channel_list": self.channel_list,
            "readings": self.readings
        }
        return json.dumps(content, indent=None)

    def merge_readings(self, new_data):
        """Any new readings found in new_data are added to self.readings"""
        for key in new_data.keys():
            if not key in self.readings:
                self.readings[key] = new_data[key]
                self.not_written += 1

    def prune_readings(self, max_length):
        """Throw away old readings"""
        n_drops = max_length - len(self.readings)
        if n_drops >= 0:
            return
        print(f'Need to prune {n_drops} readings')
        drop_candidates = self.key_list()[:-n_drops]
        for c in drop_candidates:
            del self.readings[c]

    def key_list(self):
        l = list(self.readings.keys())
        l.sort(reverse=True)
        return l

    def update(self, message):
        self.last_seen = time.time()
        seq = message.header.sequence_number
        if self.last_sequence == seq:
            # Duplicate or just added, so skip
            pass
        elif self.last_sequence + 1 == seq:
            # Message is in sequence, everything is perfect
            print(f'Processing message {message.header.sequence_number} from {message.header.device_name}')
            self.last_sequence = seq
            self.readings[seq] = message.readings[seq]
            self.message_count += 1
            self.not_written += 1
            print(f'Updated to next sequence number {seq}')
            print(f'Have readings for the following sequence numbers {self.key_list()}')
            sum_deltas = sum(r.r_delta for r in message.readings.values())
            print(f'Current average delta of poll period for device {self.name} is {sum_deltas / len(message.readings):.3f} ms')
        elif self.last_sequence + 1 < seq:
            print(f'Did not receive sequence numbers {self.last_sequence + 1}..{seq}. Trying to recover from backlog.',file=sys.stderr)
            sys.stderr.flush()
            self.last_sequence = seq
            self.merge_readings(message.readings)
            print(f'New messages merged into new readings into device {self.name}')
            print(f'Have readings for the following sequence numbers {self.key_list()}')
        else:
            # Received sequence number is smaller, assuming device reboot
            print(f'Expecting sequence number {self.last_sequence + 1} but received {message.sequence}.',
                  file=sys.stderr)
            sys.stderr.flush()


# --------------------------------------------------------------------------------
# Class Devices
# --------------------------------------------------------------------------------

class Devices:
    """All the devices sending messages with the CoReef multicast address"""
    def __init__(self, directory_for_data_files, backlog_size, write_frequency):
        self.devices = {}
        self.directory_for_data_files = directory_for_data_files
        self.backlog_size = backlog_size
        self.write_frequency = write_frequency

    def write_data_to_file(self, device):
        dt = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        p = os.path.join(self.directory_for_data_files, f"{dt}_{device.name}_data.json")
        with open(p, 'w') as fd:
            fd.write(device.to_json())

    def process_message(self, address, message):
        device_name = message.header.device_name
        if not device_name in self.devices:
            self.devices[device_name] = Device(message, address)
            print(f'New device <{device_name}> added.')
        device = self.devices[device_name]
        device.update(message)
        if device.not_written >= self.write_frequency:
            self.write_data_to_file(device)
            device.not_written = 0
        device.prune_readings(self.backlog_size)


# --------------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=str, required=False, help="The directory to store data files", default=".")
    parser.add_argument("--backlog", type=int, required=False, help="The size of the backlog storage", default=24)
    parser.add_argument("--writefreq", type=int, required=False, help="Number of readings before writing to file",
                        default=12)
    parser.add_argument("--details", type=bool, required=False, help="The size of the backlog storage", default=False)

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

    devices = Devices(data_dir, backlog_size, write_frequency)
    while True:
        rawdata, address = sock.recvfrom(1024)
        receiving_time = time.time()
        m = as_message(rawdata,receiving_time)
        devices.process_message(address,m)

if __name__ == '__main__':
    main()
