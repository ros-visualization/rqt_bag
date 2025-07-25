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
from typing import Callable, Iterable, Iterator, List, Optional, Union

from rclpy import logging
from rclpy.clock import Clock, ClockType
from rclpy.duration import Duration
from rclpy.serialization import deserialize_message
from rclpy.time import Time

import rosbag2_py
from rosbag2_py import get_default_storage_id, StorageFilter

from rosidl_runtime_py.utilities import get_message

from rqt_bag import bag_helper

WRITE_ONLY_MSG = 'open for writing only, returning None'

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
        """
        Get the (serialized) entry for a specific timestamp.

        Returns the entry that is closest in time (<=) to the provided timestamp.
        """
        if not self.reader:
            self._logger.warn('get_entry - ' + WRITE_ONLY_MSG)
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
            self._logger.warn('get_entry_after - ' + WRITE_ONLY_MSG)
            return None

        self.reader.set_read_order(rosbag2_py.ReadOrder(reverse=False))
        self.reader.seek(timestamp.nanoseconds + 1)

        # Set filter for topic of string type
        if topic:
            storage_filter = StorageFilter(topics=[topic])
            self.reader.set_filter(storage_filter)

        result = self.read_next() if self.reader.has_next() else None
        # No filter
        self.reader.reset_filter()
        return result

    def get_entries_in_range(self, t_start: Time, t_end: Time,
                             topic: Optional[Union[str, Iterable[str]]] = None,
                             progress_cb: Optional[Callable[[int], None]] = None) \
            -> Optional[List[Entry]]:
        """
        Get a list of all entries in a given time range, sorted by receive stamp.

        Do not use this function for large bags. It will load all entries into memory. Use
        entries_in_range_generator() instead and process the data as they are returned.

        :param t_start: stamp to start at, ''rclpy.time.Time''
        :param t_end: stamp to end at, ''rclpy.time.Time''
        :param topic: topic or list of topics to query (if None, all topics are), ''list(str)''
        :param progress_cb: callback function to report progress, called once per each percent.
        :returns: entries in the bag file, ''list(Entry)''
        """
        if not self.reader:
            self._logger.warn('get_entries_in_range - ' + WRITE_ONLY_MSG)
            return None

        return list(self.entries_in_range_generator(t_start, t_end, topic, progress_cb))

    def entries_in_range_generator(self, t_start: Time, t_end: Time,
                                   topic: Optional[Union[str, Iterable[str]]] = None,
                                   progress_cb: Optional[Callable[[int], None]] = None) \
            -> Iterator[Entry]:
        """
        Get a generator of all entries in a given time range, sorted by receive stamp.

        :param t_start: stamp to start at, ''rclpy.time.Time''
        :param t_end: stamp to end at, ''rclpy.time.Time''
        :param topic: topic or list of topics to query (if None, all topics are), ''list(str)''
        :param progress_cb: callback function to report progress, called once per each percent.
        :returns: generator of entries in the bag file, ''Generator(Entry)''
        """
        if not self.reader:
            self._logger.warn('entries_in_range_generator - ' + WRITE_ONLY_MSG)
            return

        if isinstance(topic, Iterable) and not isinstance(topic, str):
            topics = topic
        else:
            topics = [topic] if topic is not None else []

        self.reader.set_read_order(rosbag2_py.ReadOrder(reverse=False))
        self.reader.set_filter(rosbag2_py.StorageFilter(topics=topics))
        self.reader.seek(t_start.nanoseconds)
        if progress_cb is not None:
            num_entries = 0
            progress = 0
            estimated_num_entries = self.estimate_num_entries_in_range(t_start, t_end, topic)

        while self.reader.has_next():
            next_entry = self.read_next()
            if next_entry.timestamp <= t_end.nanoseconds:
                if progress_cb is not None:
                    num_entries += 1
                    new_progress = int(100.0 * (float(num_entries) / estimated_num_entries))
                    if new_progress != progress:
                        progress_cb(new_progress)
                        progress = new_progress
                yield next_entry
            else:
                break

        # No filter
        self.reader.reset_filter()

        if progress_cb is not None and progress != 100:
            progress_cb(100)

        return

    def estimate_num_entries_in_range(self, t_start: Time, t_end: Time,
                                      topic: Optional[Union[str, Iterable[str]]] = None) -> int:
        """
        Estimate the number of entries in the given time range.

        The computation is only approximate, based on the assumption that messages are distributed
        evenly across the whole bag on every topic.

        :param t_start: stamp to start at, ''rclpy.time.Time''
        :param t_end: stamp to end at, ''rclpy.time.Time''
        :param topic: topic or list of topics to query (if None, all topics are), ''list(str)''
        :returns: the approximate number of entries, ''int''
        """
        if not self.reader:
            self._logger.warn('estimate_num_entries_in_range - ' + WRITE_ONLY_MSG)
            return 0

        if isinstance(topic, Iterable) and not isinstance(topic, str):
            topics = topic
        else:
            topics = [topic] if topic is not None else []

        range_duration = t_end - t_start
        bag_duration = self.get_latest_timestamp() - self.get_earliest_timestamp()
        fraction = bag_helper.to_sec(range_duration) / bag_helper.to_sec(bag_duration)

        num_messages = 0
        for t_info in self.metadata.topics_with_message_count:
            if t_info.topic_metadata.name in topics:
                num_messages += t_info.message_count

        return int(fraction * num_messages)

    def read_next(self):
        return Entry(*self.reader.read_next())

    def deserialize_entry(self, entry):
        """Deserialize a bag entry into its corresponding ROS message."""
        msg_type_name = self.get_topic_type(entry.topic)
        msg_type = get_message(msg_type_name)
        ros_message = deserialize_message(entry.data, msg_type)
        return (ros_message, msg_type_name)
