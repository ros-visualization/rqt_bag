# Copyright (c) 2020, Open Source Robotics Foundation, Inc.
# All rights reserved.
#
# Software License Agreement (BSD License 2.0)
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
QoS-related utility functions
"""

import yaml
import math
import rclpy.qos

from rclpy.qos import QoSProfile
from rclpy.duration import Duration
from rclpy.time import Time


def duration_to_node(duration):
    node = {}
    node["sec"] = int(math.floor(duration.nanoseconds / 1e9))
    node["nsec"] = duration.nanoseconds % (1000 * 1000 * 1000)
    return node


def node_to_duration(node):
    return Duration(seconds=int(node["sec"]), nanoseconds=int(node["nsec"]))


def qos_profile_to_yaml(qos_profile):
    qos = {}
    qos["history"] = int(qos_profile.history)
    qos["depth"] = int(qos_profile.depth)
    qos["reliability"] = int(qos_profile.reliability)
    qos["durability"] = int(qos_profile.durability)
    qos["lifespan"] = duration_to_node(qos_profile.lifespan)
    qos["deadline"] = duration_to_node(qos_profile.deadline)
    qos["liveliness"] = int(qos_profile.liveliness)
    qos["liveliness_lease_duration"] = duration_to_node(qos_profile.liveliness_lease_duration)
    qos["avoid_ros_namespace_conventions"] = qos_profile.avoid_ros_namespace_conventions
    return yaml.dump([qos], sort_keys=False)


def yaml_to_qos_profile(qos_profile_yaml):
    qos_profiles = []
    nodes = yaml.safe_load(qos_profile_yaml)
    for node in nodes:
      qos_profile = QoSProfile(depth=int(node["depth"]))
      qos_profile.history = rclpy.qos.HistoryPolicy.RMW_QOS_POLICY_HISTORY_SYSTEM_DEFAULT
      qos_profile.reliability = int(node["reliability"])
      qos_profile.durability = int(node["durability"])
      qos_profile.liveliness = int(node["liveliness"])
      qos_profile.avoid_ros_namespace_conventions = node["avoid_ros_namespace_conventions"]
      qos_profiles.append(qos_profile)

    return qos_profiles
