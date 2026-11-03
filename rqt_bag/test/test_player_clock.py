# Copyright (c) 2026, Open Source Robotics Foundation, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#   * Redistributions of source code must retain the above copyright
#     notice, this list of conditions and the following disclaimer.
#   * Redistributions in binary form must reproduce the above
#     copyright notice, this list of conditions and the following
#     disclaimer in the documentation and/or other materials provided
#     with the distribution.
#   * Neither the name of the copyright holder nor the names of its
#     contributors may be used to endorse or promote products derived
#     from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from types import SimpleNamespace

import pytest
from rosgraph_msgs.msg import Clock
from rqt_bag import player as player_module
from rqt_bag.player import CLOCK_TOPIC, Player


class FakePublisher:

    def __init__(self, msg_type):
        self.msg_type = msg_type
        self.published = []

    def publish(self, msg):
        self.published.append(msg)


class FakeNode:

    def __init__(self):
        self.publishers = {}

    def get_logger(self):
        return SimpleNamespace(get_child=lambda name: None)

    def create_publisher(self, msg_type, topic, qos_profile):
        self.publishers[topic] = FakePublisher(msg_type)
        return self.publishers[topic]


class FakeBag:

    def deserialize_entry(self, entry):
        return (object(), None)

    def get_topic_metadata(self, topic):
        return SimpleNamespace(offered_qos_profiles=[None])


@pytest.fixture
def clock_player(monkeypatch):
    """Return a playing Player with clock publishing on, and its node."""
    monkeypatch.setattr(player_module, 'convert_rclcpp_qos_to_rclpy_qos', lambda qos: 10)
    node = FakeNode()
    player = Player(node, SimpleNamespace(play_speed=1.0))
    player.start_clock_publishing()
    return player, node


def view(player, *timestamps):
    for timestamp in timestamps:
        player.message_viewed(FakeBag(), SimpleNamespace(topic='/points', timestamp=timestamp))


def published_clock(node):
    return [(msg.clock.sec, msg.clock.nanosec) for msg in node.publishers[CLOCK_TOPIC].published]


def test_clock_publisher_uses_clock_type(clock_player):
    _, node = clock_player
    assert node.publishers[CLOCK_TOPIC].msg_type is Clock


def test_publishes_clock_with_message_timestamp(clock_player):
    player, node = clock_player
    view(player, 1_500_000_000, 2_000_000_000)
    assert published_clock(node) == [(1, 500_000_000), (2, 0)]
    assert len(node.publishers['/points'].published) == 2


def test_does_not_publish_clock_backwards(clock_player):
    player, node = clock_player
    view(player, 2_000_000_000, 1_000_000_000)
    assert published_clock(node) == [(2, 0)]
    # The message itself is still published.
    assert len(node.publishers['/points'].published) == 2


def test_publishes_older_clock_after_resume(clock_player):
    player, node = clock_player
    view(player, 2_000_000_000)
    player.resume()
    view(player, 1_000_000_000)
    assert published_clock(node) == [(2, 0), (1, 0)]
