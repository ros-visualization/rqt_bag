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

from python_qt_binding.QtCore import qDebug
from rclpy.duration import Duration
from rclpy.time import Time
from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message

import sqlite3
import os

SQL_COLUMNS = ['id', 'topic_id', 'timestamp', 'data']


class Rosbag2:
    def __init__(self, bag_info, filename):
        self.filename = filename
        self.topics = {
            topic['topic_metadata']['name']: topic
            for topic in bag_info['topics_with_message_count']
        }
        bag_dir = os.path.dirname(filename)
        database_relative_name = bag_info['relative_file_paths'][0]
        self.db_name = os.path.join(bag_dir, database_relative_name)

        self.start_time = Time(nanoseconds=bag_info['starting_time']['nanoseconds_since_epoch'])
        self.duration = Duration(nanoseconds=bag_info['duration']['nanoseconds'])

    def get_topics(self):
        return list(self.topics.keys())

    def _get_connections(self, topic_name=None):
        if topic_name is not None:
            return self.topics(topic_name)
        return dict(self.topics)

    def close(self):
        pass

    def get_topic_id(self, topic):
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

    def get_topic_info(self, topic_id):
        db = sqlite3.connect(self.db_name)
        cursor = db.cursor()
        search = cursor.execute(
            'SELECT name, type FROM topics WHERE id="{}";'.format(topic_id))
        entry = search.fetchone()
        cursor.close()
        db.close()
        if entry is None:
            return None
        return (entry[0], entry[1])

    def convert_entry_to_ros_message(self, entry):
        (topic, msg_type_name) = self.get_topic_info(entry.topic_id)
        msg_type = get_message(msg_type_name)
        ros_message = deserialize_message(entry.data, msg_type)
        return (ros_message, msg_type_name, topic)

    def _get_entry(self, timestamp, topic=None):
        qDebug("Getting entry at {} for topic {}".format(timestamp, topic))
        db = sqlite3.connect(self.db_name)
        columns_str = ", ".join(SQL_COLUMNS)
        cursor = db.cursor()
        topic_str = ''
        if topic is not None:
            # The requested topic may not be in this database
            if topic not in self.topics:
                return None
            topic_str = 'AND topic_id={} '.format(self.get_topic_id(topic))
        search = cursor.execute(
            "SELECT {} FROM messages WHERE timestamp<{} {}ORDER BY timestamp DESC LIMIT 1;".format(
            #"SELECT {} FROM messages WHERE timestamp={} {}ORDER BY timestamp DESC LIMIT 1;".format(
                columns_str, timestamp.nanoseconds, topic_str))
        entry = search.fetchone()
        cursor.close()
        db.close()
        if entry is None:
            return None

        Entry = namedtuple('Entry', SQL_COLUMNS)
        return Entry(*entry)

    def _get_entry_after(self, timestamp, topic=None):
        qDebug("Getting entry after {} for topic {}".format(timestamp, topic))
        db = sqlite3.connect(self.db_name)

        columns_str = ", ".join(SQL_COLUMNS)
        cursor = db.cursor()
        topic_str = ''
        if topic is not None:
            # The requested topic may not be in this database
            if topic not in self.topics:
                return None
            topic_str = 'AND topic_id={} '.format(self.get_topic_id(topic))
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
        qDebug("Getting entries from {} to {} for topic {}".format(t_start, t_end, topic))
        db = sqlite3.connect(self.db_name)
        columns_str = ", ".join(SQL_COLUMNS)
        cursor = db.cursor()
        topic_str = ''
        if topic is not None:
            # The requested topic may not be in this database
            if topic not in self.topics:
                return None
            topic_str = 'AND topic_id={} '.format(self.get_topic_id(topic))
        search = cursor.execute(
            "SELECT {} FROM messages WHERE timestamp>={} AND timestamp <={} {};".format(
                columns_str, t_start.nanoseconds, t_end.nanoseconds, topic_str))
        entries = search.fetchall()
        cursor.close()
        db.close()

        Entry = namedtuple('Entry', SQL_COLUMNS)
        return [Entry(*entry) for entry in entries]

    def _read_message(self, position):
        return self._get_entry(Time(nanoseconds=position))
