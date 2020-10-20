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
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICTS
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

"""
Helper functions for bag files and timestamps.
"""

from decimal import Decimal
import math
import time

import rclpy
from rclpy.duration import Duration
from rclpy.time import Time


def stamp_to_str(t):
    """
    Convert a rclpy.time.Time to a human-readable string.

    @param t: time to convert
    @type  t: rclpy.time.Time
    """
    t_sec, t_nsec = t.seconds_nanoseconds()
    if t_sec < (60 * 60 * 24 * 365 * 5):
        # Display timestamps earlier than 1975 as seconds
        return '%.3fs' % t_sec
    else:
        return time.strftime('%b %d %Y %H:%M:%S', time.localtime(t_sec)) + '.%03d' % (t_nsec * 1e-9)


def get_topics(bag):
    """
    Get an alphabetical list of all the unique topics in the bag.

    @return: sorted list of topics
    @rtype:  list of str
    """
    return sorted(bag.get_topics())


def get_start_stamp(bag):
    """
    Get the earliest timestamp in the bag.

    @param bag: bag file
    @type  bag: rosbag.Bag
    @return: earliest timestamp
    @rtype:  rclpy.time.Time
    """
    return bag.start_time


def get_end_stamp(bag):
    """
    Get the latest timestamp in the bag.

    @param bag: bag file
    @type  bag: rosbag.Bag
    @return: latest timestamp
    @rtype:  rclpy.time.Time
    """
    return bag.start_time + bag.duration


def get_topics_by_datatype(bag):
    """
    Get all the message types in the bag and their associated topics.

    @param bag: bag file
    @type  bag: rosbag.Bag
    @return: mapping from message typename to list of topics
    @rtype:  dict of str to list of str
    """
    topics_by_datatype = {}
    for name, topic in bag.topics.items():
        topics_by_datatype.setdefault(topic['topic_metadata']['type'], []).append(name)

    return topics_by_datatype


def get_datatype(bag, topic):
    """
    Get the datatype of the given topic.

    @param bag: bag file
    @type  bag: rosbag.Bag
    @return: message typename
    @rtype:  str
    """
    if topic not in bag.topics:
        return None
    return bag.topics[topic]['topic_metadata']['type']


def filesize_to_str(size):
    size_name = ('B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB')
    i = int(math.floor(math.log(size, 1024)))
    p = math.pow(1024, i)
    s = round(size / p, 2)
    if s > 0:
        return '%s %s' % (s, size_name[i])
    return '0 B'


def to_sec(t):
    """
    Convert an rclpy.time.Time or rclpy.duration.Duration to a float representing seconds

    @param t:
    @type  t: rclpy.time.Time or rclpy.duration.Duration:
    @return: result
    @rtype:  float
    """
    # 1e-9 is not exactly 1e-9 if using floating point, so doing this math with floats is imprecise
    result = Decimal(t.nanoseconds) * Decimal('1e-9')
    return float(result)
