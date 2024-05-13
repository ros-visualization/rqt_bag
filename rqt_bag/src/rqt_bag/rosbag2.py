# Copyright (c) 2019, PickNik Consulting.
# Copyright (c) 2020, Open Source Robotics Foundation, Inc.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#
#    * Neither the name of the Willow Garage nor the names of its
#      contributors may be used to endorse or promote products derived from
#      this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

"""A rosbag abstraction with functionality required by rqt_bag."""

from collections import namedtuple
import os

from rclpy.clock import Clock, ClockType
from rclpy.duration import Duration
from rclpy import logging
from rclpy.serialization import deserialize_message
import rosbag2_py
from rosidl_runtime_py.utilities import get_message

from rosbag2_py import get_default_storage_id, StorageFilter

WRITE_ONLY_MSG = "open for writing only, returning None"

Entry = namedtuple('Entry', ['topic', 'data', 'timestamp'])


class Rosbag2:

    def __init__(self, bag_path, recording=False, topics={},
                 serialization_format='cdr', storage_id=get_default_storage_id()):
        self.bag_path = bag_path
        self.reader = None
        self._logger = logging.get_logger('rqt_bag.Rosbag2')

        if recording:
            self.metadata = rosbag2_py.BagMetadata()
            self.metadata.starting_time = Clock(clock_type=ClockType.SYSTEM_TIME).now()
            self.metadata.duration = Duration(nanoseconds=0)
            self.topic_metadata_map = topics
        else:
            self.reader = rosbag2_py.SequentialReader()
            self.reader.open(
                rosbag2_py.StorageOptions(uri=bag_path), rosbag2_py.ConverterOptions())
            self.metadata = self.reader.get_metadata()
            self.db_name = os.path.join(self.bag_path, self.metadata.relative_file_paths[0])
            self.topic_metadata_map = {
                t_info.topic_metadata.name: t_info.topic_metadata
                for t_info in self.metadata.topics_with_message_count
            }
            self.read_order = rosbag2_py.ReadOrder(
                sort_by=rosbag2_py.ReadOrderSortBy.ReceivedTimestamp,
                reverse=False)

    def size(self):
        """Get the size of the rosbag."""
        return self.metadata.bag_size

    def get_earliest_timestamp(self):
        """Get the timestamp of the earliest message in the bag."""
        return self.metadata.starting_time

    def get_latest_timestamp(self):
        """Get the timestamp of the most recent message in the bag."""
        return self.metadata.starting_time + self.metadata.duration

    def set_latest_timestamp(self, t):
        self.metadata.duration = t - self.metadata.starting_time

    def get_topics(self):
        """Get all of the topics used in this bag."""
        return sorted(self.topic_metadata_map.keys())

    def get_topic_type(self, topic):
        """Get the topic type for a given topic name."""
        if topic not in self.topic_metadata_map:
            return None
        return self.topic_metadata_map[topic].type

    def get_topic_metadata(self, topic):
        """Get the full metadata for a given topic name."""
        if topic not in self.topic_metadata_map:
            return None
        return self.topic_metadata_map[topic]

    def get_topics_by_type(self):
        """Return a map of topic data types to a list of topics publishing that type."""
        topics_by_type = {}
        for name, topic in self.topic_metadata_map.items():
            topics_by_type.setdefault(topic.type, []).append(name)
        return topics_by_type

    def get_entry(self, timestamp, topic=None):
        """Get the (serialized) entry for a specific timestamp.

        Returns the entry that is closest in time (<=) to the provided timestamp.
        """
        if not self.reader:
            self._logger.warn("get_entry - " + WRITE_ONLY_MSG)
            return None

        self.reader.set_read_order(rosbag2_py.ReadOrder(reverse=True))
        self.reader.seek(timestamp.nanoseconds)

        # Set filter for topic of string type
        if topic:
            storage_filter = StorageFilter(topics=[topic])
            self.reader.set_filter(storage_filter)

        result = self.read_next() if self.reader.has_next() else None
        # No filter
        self.reader.reset_filter()
        return result

    def get_entry_after(self, timestamp, topic=None):
        """Get the next entry after a given timestamp."""
        if not self.reader:
            self._logger.warn("get_entry_after - " + WRITE_ONLY_MSG)
            return None

        self.reader.set_read_order(rosbag2_py.ReadOrder(reverse=False))
        self.reader.seek(timestamp.nanoseconds + 1)
        return self.read_next() if self.reader.has_next() else None

    def get_entries_in_range(self, t_start, t_end, topic=None):
        if not self.reader:
            self._logger.warn("get_entries_in_range - " + WRITE_ONLY_MSG)
            return None

        self.reader.set_read_order(rosbag2_py.ReadOrder(reverse=False))
        self.reader.set_filter(rosbag2_py.StorageFilter(topics=[topic] if topic else []))
        self.reader.seek(t_start.nanoseconds)
        entries = []
        while self.reader.has_next():
            next_entry = self.read_next()
            if next_entry.timestamp <= t_end.nanoseconds:
                entries.append(next_entry)
            else:
                break

        return entries

    def read_next(self):
        return Entry(*self.reader.read_next())

    def deserialize_entry(self, entry):
        """Deserialize a bag entry into its corresponding ROS message."""
        msg_type_name = self.get_topic_type(entry.topic)
        msg_type = get_message(msg_type_name)
        ros_message = deserialize_message(entry.data, msg_type)
        return (ros_message, msg_type_name)
