# Software License Agreement (BSD License)
#
# Copyright (c) 2019, PickNik Consulting.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#  * Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above
#    copyright notice, this list of conditions and the following
#    disclaimer in the documentation and/or other materials provided
#    with the distribution.
#  * Neither the name of Willow Garage, Inc. nor the names of its
#    contributors may be used to endorse or promote products derived
#    from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

"""
This is an example Python interface for rosbag2 to be replaced in the future by a proper
implementation.
"""

from collections import namedtuple

from rclpy.duration import Duration
from rclpy.time import Time

import sqlite3
import os
import pathlib
import yaml
import rosbag2_py

from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message

SQL_COLUMNS = ['id', 'topic_id', 'timestamp', 'data']


class Rosbag2:
    def __init__(self, bag_path, serialization_format = 'cdr', storage_id = 'sqlite3'):
        self.bag_path = bag_path

        with open(bag_path + '/metadata.yaml') as f:
            full_bag_info  = yaml.safe_load(f)
            bag_info = full_bag_info['rosbag2_bagfile_information']

            self.topics = {
                topic['topic_metadata']['name']: topic
                for topic in bag_info['topics_with_message_count']
            }

            database_relative_name = bag_info['relative_file_paths'][0]
            self.db_name = os.path.join(self.bag_path, database_relative_name)

            self.start_time = Time(nanoseconds=bag_info['starting_time']['nanoseconds_since_epoch'])
            self.duration = Duration(nanoseconds=bag_info['duration']['nanoseconds'])

        self.serialization_format = serialization_format
        self.storage_id = storage_id
        storage_options = rosbag2_py.StorageOptions(uri=self.bag_path, storage_id=self.storage_id)

        converter_options = rosbag2_py.ConverterOptions(
            input_serialization_format=self.serialization_format,
            output_serialization_format=self.serialization_format)

        self.reader = rosbag2_py.SequentialReader()
        self.reader.open(storage_options, converter_options)

        self.topic_types = self.reader.get_all_topics_and_types()

        # Create maps for quicker lookup
        self.topic_type_map = {self.topic_types[i].name: self.topic_types[i].type for i in range(len(self.topic_types))}
        self.topic_metadata_map = {self.topic_types[i].name: self.topic_types[i] for i in range(len(self.topic_types))}

        #print(self.topic_type_map)

        #for topic_metadata in self.topic_types:
        #    print("name: {}".format(topic_metadata.name))
        #    print("type: {}".format(topic_metadata.type))
        #    print("serialization_format: {}".format(topic_metadata.serialization_format))
        #    print("offered_qos_profiles: {}".format(topic_metadata.offered_qos_profiles))

#########################################

    def close(self):
        pass

    def size(self):
        return sum(f.stat().st_size for f in pathlib.Path(self.bag_path).glob('**/*') if f.is_file())

    def get_topics(self):
        return list(self.topics.keys())

    def get_topic_type(self, topic):
        return self.topic_type_map[topic]

#########################################

    def get_topic_metadata2(self, topic):
        return self.topic_metadata_map[topic]

    def get_topic_metadata(self, topic_id):
        db = sqlite3.connect(self.db_name)
        cursor = db.cursor()
        search = cursor.execute(
            'SELECT name, type, serialization_format, offered_qos_profiles FROM topics WHERE id="{}";'.format(topic_id))
        entry = search.fetchone()
        cursor.close()
        db.close()
        if entry is None:
            return None
        return (entry[0], entry[1], entry[2], entry[3])

#########################################

    def _get_entry(self, timestamp, topic=None):
        db = sqlite3.connect(self.db_name)
        cursor = db.cursor()
        topic_str = ''
        if topic is not None:
            # The requested topic may not be in this database
            if topic not in self.topics:
                return None
            topic_str = 'AND topic_id={} '.format(self._get_topic_id(topic))

        search = cursor.execute(
            "SELECT messages.id, topic_id, timestamp, data, topics.name FROM messages INNER JOIN topics ON messages.topic_id=topics.id WHERE timestamp<={} {}ORDER BY timestamp DESC LIMIT 1;".format(timestamp.nanoseconds, topic_str))
        entry = search.fetchone()
        cursor.close()
        db.close()
        if entry is None:
            return None

        Entry = namedtuple('Entry', ['id', 'topic_id', 'timestamp', 'data', 'name'])
        foobar = Entry(*entry)
        
        # TODO: change 'name' to 'topic'
        FullEntry = namedtuple('FullEntry', ['id', 'topic_id', 'timestamp', 'data', 'name', 'ros_message'])

        #(topic, msg_type_name, _, _) = bag.get_topic_metadata(entry.topic_id)

        #print("foobar.name: {}".format(foobar.name))
        #print("foobar.type: {}".format(self.topic_type_map[foobar.name]))

        msg_type = get_message(self.topic_type_map[foobar.name])
        ros_message = deserialize_message(foobar.data, msg_type)

        # TODO: change 'name' to 'topic'
        # TODO: remove conversion to 'ros_message' 
        # rosbag_reader returns: (topic(name), data, timestamp) and can then get msg_type and msg)
        return FullEntry(foobar.id, foobar.topic_id, foobar.timestamp, foobar.data, foobar.name, ros_message)

    def _get_entry_after(self, timestamp, topic=None):
        db = sqlite3.connect(self.db_name)

        columns_str = ", ".join(SQL_COLUMNS)
        cursor = db.cursor()
        topic_str = ''
        if topic is not None:
            # The requested topic may not be in this database
            if topic not in self.topics:
                return None
            topic_str = 'AND topic_id={} '.format(self._get_topic_id(topic))
        search = cursor.execute(
            "SELECT {} FROM messages WHERE timestamp>{} {}LIMIT 1;".format(
                columns_str, timestamp.nanoseconds, topic_str))
        entry = search.fetchone()
        cursor.close()
        db.close()
        if entry is None:
            return None

        Entry = namedtuple('Entry', SQL_COLUMNS)
        return Entry(*entry)

    def _get_entries(self, t_start, t_end, topic=None):
        db = sqlite3.connect(self.db_name)
        columns_str = ", ".join(SQL_COLUMNS)
        cursor = db.cursor()
        topic_str = ''
        if topic is not None:
            # The requested topic may not be in this database
            if topic not in self.topics:
                return None
            topic_str = 'AND topic_id={} '.format(self._get_topic_id(topic))
        search = cursor.execute(
            "SELECT {} FROM messages WHERE timestamp>={} AND timestamp <={} {};".format(
                columns_str, t_start.nanoseconds, t_end.nanoseconds, topic_str))
        entries = search.fetchall()
        cursor.close()
        db.close()

        Entry = namedtuple('Entry', SQL_COLUMNS)
        return [Entry(*entry) for entry in entries]

    def _get_topic_id(self, topic):
        db = sqlite3.connect(self.db_name)
        cursor = db.cursor()
        search = cursor.execute(
            'SELECT id FROM topics WHERE name="{}";'.format(topic))
        entry = search.fetchone()
        cursor.close()
        db.close()
        if entry is None:
            return None
        return entry[0]

