# Copyright (c) 2026, Rahul Seervi
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
from rqt_bag import player as player_module
from rqt_bag.player import Player


class FakePublisher:

    def __init__(self):
        self.published = []

    def publish(self, msg):
        self.published.append(msg)


class FakeNode:

    def __init__(self):
        self.publishers = {}

    def get_logger(self):
        return SimpleNamespace(get_child=lambda name: None)

    def create_publisher(self, msg_type, topic, qos_profile):
        self.publishers[topic] = FakePublisher()
        return self.publishers[topic]


class FakeBag:

    def __init__(self, msg):
        self._msg = msg

    def deserialize_entry(self, entry):
        return (self._msg, None)

    def get_topic_metadata(self, topic):
        return SimpleNamespace(offered_qos_profiles=[None])


@pytest.fixture
def viewed(monkeypatch):
    """Show a message to a Player at the given play speed, and return what was published."""
    monkeypatch.setattr(player_module, 'convert_rclcpp_qos_to_rclpy_qos', lambda qos: 10)

    def view(play_speed):
        node = FakeNode()
        player = Player(node, SimpleNamespace(play_speed=play_speed))
        msg = object()
        player.message_viewed(FakeBag(msg), SimpleNamespace(topic='/points', timestamp=0))
        published = node.publishers['/points'].published if '/points' in node.publishers else []
        return msg, published

    return view


def test_publishes_while_playing(viewed):
    msg, published = viewed(1.0)
    assert published == [msg]


def test_publishes_message_stepped_to_while_paused(viewed):
    # navigate_next() and navigate_previous() stop playback before moving the playhead.
    msg, published = viewed(0.0)
    assert published == [msg]


def test_does_not_publish_while_playing_in_reverse(viewed):
    _, published = viewed(-1.0)
    assert published == []
