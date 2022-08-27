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

import paho.mqtt.client as mqtt
mqtt_address = '10.42.1.102'
mqtt_port = 1884

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

def epoch_to_string(e):
    return datetime.fromtimestamp(e).strftime("%Y.%m.%d %H:%M:%S")

# --------------------------------------------------------------------------------
# Class Device
# --------------------------------------------------------------------------------

class Device:
    """Device keeps track of all state information and limited number of readings"""

    def __init__(self, message, address, max_readings):
        """Most state information for a new device is part of any message"""
        self.max_readings = max_readings
        self.init_with_message(message,address)
        print(f'Have readings for the following sequence numbers {self.readings.keys()}')

    def init_with_message(self,message,address):
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

    def vitals(self):
        """Create a dictionary of all relevant device vitals"""
        v = {
            "Address" : self.address,
            "Last Sequence" : self.last_sequence,
            "Last Seen Epoch" : self.last_seen,
            "Last Seen" : epoch_to_string(self.last_seen),
            "Not Written" : self.not_written,
            "Message Count" : self.message_count,
            "Free Heap" : self.free_heap
        }
        return v

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
            "channel_list": self.channel_list
        }
        pretty_r = []
        for r in self.readings.values():
            ts = self.first_seen_l + (r.r_timebase + r.r_delta - self.first_seen_r)/1000.0
            dt = epoch_to_string(ts)
            pretty_r.append((r.sequence,dt,r.r_timebase,r.r_delta,r.values))
        pretty_r.sort(key=lambda x : x[0],reverse=True)
        content["readings"] = pretty_r
        return json.dumps(content, indent=2)
    
    def mqtt_publish(self, mqtt_client):
        """Create JSON string for last reading and publish it to MQTT server"""
        content = { "Timestamp" : datetime.fromtimestamp(self.last_seen).strftime("%Y.%m.%d %H:%M:%S") }
        for i, channel_name in enumerate(self.channel_list):
            content[channel_name] = self.readings[self.last_sequence].values[i]
        content["Sequence"] = self.last_sequence
        mqtt_message = json.dumps(content, indent=None)
        mqtt_tag = f'{self.name}/reading'
        print(f'Sending <{mqtt_message}> with tag <{mqtt_tag} to MQTT-Server {mqtt_client}')
        mqtt_client.publish(mqtt_tag,mqtt_message)
        

    def merge_readings(self, new_data):
        """Any new readings found in new_data are added to self.readings"""
        for key in new_data.keys():
            if not key in self.readings:
                self.readings[key] = new_data[key]
                self.not_written += 1

    def update(self, message, address):
        """Check whether new message is duplicate, in sequence, out of sequence or from the past"""
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
            print(f'Have readings for the following sequence numbers {self.readings.keys()}')
            sum_deltas = sum(r.r_delta for r in message.readings.values())
            print(f'Current average delta of poll period for device {self.name} is {sum_deltas / len(message.readings):.3f} ms')
        elif self.last_sequence + 1 < seq:
            print(f'Did not receive sequence numbers {self.last_sequence + 1}..{seq}. Trying to recover from backlog.',file=sys.stderr)
            sys.stderr.flush()
            self.last_sequence = seq
            self.merge_readings(message.readings)
            self.message_count += 1
            print(f'New messages merged into new readings into device {self.name}')
            print(f'Have readings for the following sequence numbers {self.key_list()}')
        else:
            # Received sequence number is smaller, assuming device reboot
            print(f'Expecting sequence number {self.last_sequence + 1} but received {message.header.sequence_number}.',file=sys.stderr)
            sys.stderr.flush()
            self.init_with_message(message,address)
        self.prune_readings()

    def prune_readings ( self ):
        """Ensure that at most self.max_readings number of readings are stored"""
        n_drops = self.max_readings - len(self.readings)
        if n_drops >= 0:
            return
        print(f'Need to prune {-n_drops} readings')
        all_keys = list(self.readings.keys())
        all_keys.sort(reverse=True)
        drop_candidates = all_keys[n_drops:]
        print(f'Candidates to drop are {drop_candidates}')
        for c in drop_candidates:
            del self.readings[c]

        


# --------------------------------------------------------------------------------
# Class Devices
# --------------------------------------------------------------------------------

class Devices:
    """All the devices sending messages with the CoReef multicast address"""
    def __init__(self, directory_for_data_files, backlog_size, write_frequency, mqtt_client):
        self.devices = {}
        self.configure(directory_for_data_files,backlog_size,write_frequency,mqtt_client)

    def __init__(self):
        """A default constructor becasue a global variable of this class is needed"""
        self.devices = {}

    def configure(self, directory_for_data_files, backlog_size, write_frequency, mqtt_client):
        """Configuration parameter are delivered later - Surely, Python has a better way to solve this problem?"""
        self.directory_for_data_files = directory_for_data_files
        self.backlog_size = backlog_size
        self.write_frequency = write_frequency
        self.mqtt_client = mqtt_client


    def write_data_to_file(self, device):
        """Write all the collected state of a device to file for house keeping"""
        dt = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        p = os.path.join(self.directory_for_data_files, f"{dt}_{device.name}_data.json")
        with open(p, 'w') as fd:
            fd.write(device.to_json())

    def process_message(self, address, message):
        device_name = message.header.device_name
        if not device_name in self.devices:
            self.devices[device_name] = Device(message, address, self.backlog_size)
            print(f'New device <{device_name}> added.')
        device = self.devices[device_name]
        device.update(message,address)
        device.mqtt_publish(self.mqtt_client)
        if device.not_written >= self.write_frequency:
            self.write_data_to_file(device)
            device.not_written = 0

    def send_briefing(self):
        content = {
            "Device Count" : len(self.devices),
            "Directory" : self.directory_for_data_files,
            "Backlog Size": self.backlog_size,
            "Write Frequency": self.write_frequency
        }
        for d in self.devices.values():
            d.mqtt_publish(self.mqtt_client)
            d_vitals = d.vitals()
            content[d.name] = d_vitals
        mqtt_message = json.dumps(content, indent=None)
        mqtt_tag = f'picker/briefing'
        print(f'Sending <{mqtt_message}> with tag <{mqtt_tag} to MQTT-Server {self.mqtt_client}')
        self.mqtt_client.publish(mqtt_tag,mqtt_message)

devices = Devices()

# --------------------------------------------------------------------------------
# MQTT
# --------------------------------------------------------------------------------

def on_connect(client, userdata, flags, rc):
    client.subscribe('briefing')

def on_message(client, userdata, message):
    global devices
    m_raw = message.payload.decode("utf-8")
    m = json.loads(m_raw)
    print(f'Received <{m}> from MQTT server')
    scope_all = m.get('Scope',False)
    if scope_all:
        devices.send_briefing()

# --------------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------------

def main():
    global devices
    parser = argparse.ArgumentParser()
    parser.add_argument("--outdir", type=str, required=False, help="The directory to store data files", default=".")
    parser.add_argument("--backlog", type=int, required=False, help="The size of the backlog storage", default=24)
    parser.add_argument("--writefreq", type=int, required=False, help="Number of readings before writing to file", default=12)
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

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(mqtt_address,mqtt_port,60)
    client.loop_start()

    devices.configure(data_dir, backlog_size, write_frequency, client)
    while True:
        rawdata, address = sock.recvfrom(1024)
        receiving_time = time.time()
        m = as_message(rawdata,receiving_time)
        devices.process_message(address,m)

if __name__ == '__main__':
    main()
