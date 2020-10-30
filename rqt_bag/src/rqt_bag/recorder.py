# Software License Agreement (BSD License)
#
# Copyright (c) 2012, Willow Garage, Inc.
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
Recorder subscribes to ROS messages and writes them to a bag file.
"""

from __future__ import print_function
try:
    from queue import Queue
    from queue import Empty
except ImportError:
    from Queue import Queue
    from Queue import Empty
import re
import threading
import time
import sys
import rosbag2_py
import yaml
import os

from rosbag2_transport import rosbag2_transport_py
from .rosbag2 import Rosbag2
from rclpy.qos import qos_profile_system_default
from rclpy.topic_or_service_is_hidden import topic_or_service_is_hidden
from rosidl_runtime_py.utilities import get_message
from rclpy.serialization import serialize_message
from rclpy.duration import Duration
from rqt_bag import bag_helper
from rclpy.time import Time
from rclpy.clock import Clock, ClockType
from rclpy.qos import QoSHistoryPolicy, QoSDurabilityPolicy, QoSReliabilityPolicy, QoSLivelinessPolicy


class Recorder(object):

    def __init__(self, node, filename, bag_lock=None, all=True, topics=[], regex=False, limit=0,
                 master_check_interval=1.0):
        """
        Subscribe to ROS messages and record them to a bag file.

        @param filename: filename of bag to write to
        @type  filename: str
        @param all: all topics are to be recorded [default: True]
        @type  all: bool
        @param topics: topics (or regexes if regex is True) to record [default: empty list]
        @type  topics: list of str
        @param regex: topics should be considered as regular expressions [default: False]
        @type  regex: bool
        @param limit: record only this number of messages on each topic (if non-positive, then
            unlimited) [default: 0]
        @type  limit: int
        @param master_check_interval: period (in seconds) to check master for new topic
            publications [default: 1]
        @type  master_check_interval: float
        """
        self._node = node
        self._all = all
        self._topics = topics
        self._regex = regex
        self._limit = limit
        self._master_check_interval = master_check_interval
        self._serialization_format='cdr'
        self._storage_id = 'sqlite3'
        self._bag_filename = filename

        ################################################################
        # TODO(mjeronimo): Need to unify the direct database access of the current Rosbag2
        # implementation and the rosbag_writer into a single Rosbag2 class
        storage_options = rosbag2_py.StorageOptions(uri=filename, storage_id=self._storage_id)
        converter_options = rosbag2_py.ConverterOptions(
            input_serialization_format=self._serialization_format,
            output_serialization_format=self._serialization_format)
        self._rosbag_writer = rosbag2_py.SequentialWriter()
        self._rosbag_writer.open(storage_options, converter_options)

        # TODO(mjeronimo):
        # A hack here to create a metadata dictionary (usually read from the one created for the
        # database) sufficient to create a Rosbag2 object. The rosbag_writer won't create this file
        # until the object is destroyed. Unfortunately, the writer is kept open so that it can write
        # out messages as they are received.
        bag_info = {}
        bag_info['topics_with_message_count'] = []
        for topic, msg_type_names in self.get_topic_names_and_types():
            if topic in topics:
                topic_info = {}
                topic_info['topic_metadata'] = {}
                topic_info['topic_metadata']['name'] = topic

                # TODO(mjeronimo): could have multiple type names
                topic_info['topic_metadata']['type'] = msg_type_names[0]
                topic_info['topic_metadata']['serialization_format'] = self._serialization_format

                # TODO(mjeronimo): add the offered_qos_profiles
                topic_info['topic_metadata']['offered_qos_profiles'] = ""
                bag_info['topics_with_message_count'].append(topic_info)

                # Add the topic to the database
                # TODO(mjeronimo): need qos info here when creating the topic
                topic_metadata = rosbag2_py.TopicMetadata(name=topic, type=msg_type_names[0],
                                                          serialization_format=self._serialization_format)

                self._rosbag_writer.create_topic(topic_metadata)

        topic_info['message_count'] = 0
        bag_info['relative_file_paths'] = []
        bag_name = os.path.split(filename)[1]
        bag_info['relative_file_paths'].append(bag_name + "_0.db3")
        bag_info['starting_time'] = {}
        now = Clock(clock_type=ClockType.SYSTEM_TIME).now()
        bag_info['starting_time']['nanoseconds_since_epoch'] = now.nanoseconds
        bag_info['duration'] = {}
        bag_info['duration']['nanoseconds'] = 0

        self._bag = Rosbag2(bag_info, filename + "/metadata.yaml")
        ################################################################

        self._bag_lock = bag_lock if bag_lock else threading.Lock()
        self._listeners = []
        self._subscriber_helpers = {}
        self._limited_topics = set()
        self._failed_topics = set()
        self._last_update = time.time()
        self._write_queue = Queue()
        self._paused = False
        self._stop_condition = threading.Condition()
        self._stop_flag = False

        # Compile regular expressions
        if self._regex:
            self._regexes = [re.compile(t) for t in self._topics]
        else:
            self._regexes = None

        self._message_count = {}  # topic -> int (track number of messages recorded on each topic)
        self._master_check_thread = threading.Thread(target=self._run_master_check)
        self._write_thread = threading.Thread(target=self._run_write)

    def add_listener(self, listener):
        """
        Add a listener which gets called whenever a message is recorded.
        @param listener: function to call
        @type  listener: function taking (topic, message, time)
        """
        self._listeners.append(listener)

    def start(self):
        """
        Start subscribing and recording messages to bag.
        """
        self._master_check_thread.start()
        self._write_thread.start()

    @property
    def paused(self):
        return self._paused

    def pause(self):
        self._paused = True

    def unpause(self):
        self._paused = False

    def toggle_paused(self):
        self._paused = not self._paused

    def stop(self):
        """
        Stop recording.
        """
        with self._stop_condition:
            self._stop_flag = True
            self._stop_condition.notify_all()

        self._write_queue.put(self)

    # Implementation

    def get_topic_names_and_types(self, include_hidden_topics=False):
        topic_names_and_types = self._node.get_topic_names_and_types()
        if not include_hidden_topics:
            topic_names_and_types = [
                (n, t) for (n, t) in topic_names_and_types
                if not topic_or_service_is_hidden(n)]
        return topic_names_and_types


    def _run_master_check(self):
        try:
            while not self._stop_flag:
                # Check for new topics
                for topic, msg_type_names in self.get_topic_names_and_types():
                    # Check if:
                    #    the topic is already subscribed to, or
                    #    we've failed to subscribe to it already, or
                    #    we've already reached the message limit, or
                    #    we don't want to subscribe
                    for msg_type_name in msg_type_names:
                        if topic in self._subscriber_helpers or \
                                topic in self._failed_topics or \
                                topic in self._limited_topics or \
                                not self._should_subscribe_to(topic):
                            continue

                        try:
                            self._message_count[topic] = 0
                            self._subscriber_helpers[topic] = _SubscriberHelper(self._node, self, topic, msg_type_name)
                        except Exception as ex:
                            print('Error subscribing to %s (ignoring): %s' %
                                  (topic, str(ex)), file=sys.stderr)
                            self._failed_topics.add(topic)

                # Wait a while
                self._stop_condition.acquire()
                self._stop_condition.wait(self._master_check_interval)

        except Exception as ex:
            print('Error recording to bag: %s' % str(ex), file=sys.stderr)

        # Unsubscribe from all topics
        for topic in list(self._subscriber_helpers.keys()):
            self._unsubscribe(topic)

    def _should_subscribe_to(self, topic):
        if self._all:
            return True

        if not self._regex:
            return topic in self._topics

        for regex in self._regexes:
            if regex.match(topic):
                return True

        return False

    def _unsubscribe(self, topic):
        try:
             self._node.destroy_subscription(self, self._subscriber_helpers[topic].subscriber)
        except Exception:
            return

        del self._subscriber_helpers[topic]

    def _record(self, topic, msg, msg_type_name):
        if self._paused:
            return

        if self._limit and self._message_count[topic] >= self._limit:
            self._limited_topics.add(topic)
            self._unsubscribe(topic)
            return

        now = Clock(clock_type=ClockType.SYSTEM_TIME).now()
        self._write_queue.put((topic, msg, msg_type_name, now))
        self._message_count[topic] += 1

    def _run_write(self):
        try:
            poll_interval = 1.0
            while not self._stop_flag:
                try:
                    item = self._write_queue.get(block=False)
                except Empty:
                    time.sleep(poll_interval)
                    continue

                if item == self:
                    continue

                topic, msg, msg_type_name, t = item

                # Write to the bag
                with self._bag_lock:
                    # HERE:
                    helper = self._subscriber_helpers[topic]
                    #print(str(helper.qos_profile))

                    qos_dict = {}
                    qos_dict["history"] = 0 # helper.qos_profile.history
                    qos_dict["depth"] = helper.qos_profile.depth
                    qos_dict["reliability"] = 1 # helper.qos_profile.reliability
                    qos_dict["durability"] = 1 # helper.qos_profile.durability
                    qos_dict["lifespan"] = 0 # helper.qos_profile.lifespan
                    qos_dict["deadline"] = 0 # helper.qos_profile.deadline
                    qos_dict["liveliness"] = 1 # helper.qos_profile.liveliness
                    qos_dict["liveliness_lease_duration"] = 0 # helper.qos_profile.liveliness_lease_duration
                    qos_dict["avoid_ros_namespace_conventions"] = helper.qos_profile.avoid_ros_namespace_conventions

                    print(qos_dict)
                    qos_profile_yaml = yaml.dump([qos_dict], sort_keys=False)

                    topic_metadata = rosbag2_py.TopicMetadata(name=topic, type=msg_type_name, serialization_format=self._serialization_format, offered_qos_profiles=qos_profile_yaml)
                    self._rosbag_writer.create_topic(topic_metadata)

                    serialized_msg = serialize_message(msg)
                    self._rosbag_writer.write(topic, serialized_msg, t.nanoseconds)

                    duration_ns = t.nanoseconds - self._bag.start_time.nanoseconds
                    self._bag.duration = Duration(nanoseconds=duration_ns)

                # Notify listeners that a message has been recorded
                for listener in self._listeners:
                    listener(topic, msg, t)

        except Exception as ex:
            print('Error write to bag: %s' % str(ex), file=sys.stderr)


class _SubscriberHelper(object):

    def __init__(self, node, recorder, topic, msg_type_name):
        self.node = node
        self.recorder = recorder
        self.topic = topic
        self.msg_type_name = msg_type_name
        self.msg_type = get_message(msg_type_name)

        # Attempt to use the same QoS settings as the publisher (?)
        info = node.get_publishers_info_by_topic(topic)
        if info:
            self.qos_profile = info[0].qos_profile

            self.qos_profile.history = QoSHistoryPolicy.RMW_QOS_POLICY_HISTORY_SYSTEM_DEFAULT
            self.qos_profile.depth = 0
            #self.qos_profile.reliability = QoSReliabilityPolicy.RMW_QOS_POLICY_RELIABILITY_SYSTEM_DEFAULT
            #self.qos_profile.durability = QoSDurabilityPolicy.RMW_QOS_POLICY_DURABILITY_SYSTEM_DEFAULT
            self.qos_profile.lifespan = Duration(nanoseconds=0)
            self.qos_profile.deadline = Duration(nanoseconds=0)
            #self.qos_profile.liveliness = QoSLivelinessPolicy.RMW_QOS_POLICY_LIVELINESS_SYSTEM_DEFAULT
            self.qos_profile.liveliness_lease_duration = Duration(nanoseconds=0)
            #self.qos_profile.avoid_ros_namespace_conventions = False

            self.subscriber = node.create_subscription(self.msg_type, topic, self.callback, self.qos_profile)


    def callback(self, msg):
        self.recorder._record(self.topic, msg, self.msg_type_name)



#print("topic: {}".format(topic))
#print(" my qos")
#print(self.qos_profile)
#print("\n")
#print(" system_default:")
#print(qos_profile_system_default)

#
# std::string Recorder::serialized_offered_qos_profiles_for_topic(const std::string & topic_name)
# {
#   YAML::Node offered_qos_profiles;
#   auto endpoints = node_->get_publishers_info_by_topic(topic_name);
#   for (const auto & info : endpoints) {
#     offered_qos_profiles.push_back(Rosbag2QoS(info.qos_profile()));
#   }
#   return YAML::Dump(offered_qos_profiles);
# }
# 
# rclcpp::QoS Recorder::subscription_qos_for_topic(const std::string & topic_name) const
# {
#   if (topic_qos_profile_overrides_.count(topic_name)) {
#     return topic_qos_profile_overrides_.at(topic_name);
#   }
#   return Rosbag2QoS::adapt_request_to_offers(
#     topic_name, node_->get_publishers_info_by_topic(topic_name));
# }
# 
