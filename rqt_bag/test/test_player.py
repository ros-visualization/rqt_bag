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
from rclpy.time import Time
from rqt_bag import player as player_module
from rqt_bag.bag_timeline import BagTimeline
from rqt_bag.player import Player


class FakePublisher:

    def __init__(self, published):
        self._published = published

    def publish(self, msg):
        self._published.append(msg)


class FakeNode:

    def __init__(self):
        self.published = []

    def get_logger(self):
        return SimpleNamespace(get_child=lambda name: None)

    def create_publisher(self, msg_type, topic, qos_profile):
        return FakePublisher(self.published)


class FakeBag:
    """A bag holding entries with a topic, a timestamp in nanoseconds and a message."""

    def __init__(self, entries):
        self.entries = sorted(entries, key=lambda entry: entry.timestamp)

    def deserialize_entry(self, entry):
        return (entry.msg, None)

    def get_topic_metadata(self, topic):
        return SimpleNamespace(offered_qos_profiles=[None])


class FakeTimeline:
    """Runs BagTimeline's navigation over a FakeBag, without Qt or a real bag."""

    navigate_stop = BagTimeline.navigate_stop
    navigate_next = BagTimeline.navigate_next
    navigate_previous = BagTimeline.navigate_previous
    get_next_message_time = BagTimeline.get_next_message_time
    get_previous_message_time = BagTimeline.get_previous_message_time

    def __init__(self, bag, playhead, play_speed=0.0):
        self.bag = bag
        self.play_speed = play_speed
        self._play_timer = SimpleNamespace(stop=lambda: None)
        self._timeline_frame = SimpleNamespace(
            playhead=Time(nanoseconds=playhead),
            _start_stamp=Time(nanoseconds=bag.entries[0].timestamp),
            _end_stamp=Time(nanoseconds=bag.entries[-1].timestamp))
        self._player = None

    def add_listener(self, topic, listener):
        pass

    def get_entry(self, t, topic):
        entries = [e for e in self.bag.entries
                   if e.topic == topic and e.timestamp <= t.nanoseconds]
        return (self.bag, entries[-1]) if entries else (None, None)

    def get_entry_before(self, t):
        entries = [e for e in self.bag.entries if e.timestamp < t.nanoseconds]
        return (self.bag, entries[-1]) if entries else (None, None)

    def get_entry_after(self, t):
        entries = [e for e in self.bag.entries if e.timestamp > t.nanoseconds]
        return (self.bag, entries[0]) if entries else (None, None)

    def view_playhead(self, topic):
        """Show the Player the message under the playhead, as MessageListenerThread does."""
        bag, entry = self.get_entry(self._timeline_frame.playhead, topic)
        self._player.message_viewed(bag, entry)


def entry(topic, timestamp):
    return SimpleNamespace(topic=topic, timestamp=timestamp, msg=f'{topic}@{timestamp}')


@pytest.fixture
def make_player(monkeypatch):
    """Return a Player publishing the given topics of a FakeTimeline, and what it published."""
    monkeypatch.setattr(player_module, 'convert_rclcpp_qos_to_rclpy_qos', lambda qos: 10)

    def make(timeline, topics):
        node = FakeNode()
        timeline._player = Player(node, timeline)
        for topic in topics:
            timeline._player.start_publishing(topic)
        return node.published

    return make


def test_publishes_while_playing(make_player):
    timeline = FakeTimeline(FakeBag([entry('/a', 1)]), playhead=1, play_speed=1.0)
    published = make_player(timeline, ['/a'])
    timeline.view_playhead('/a')
    assert published == ['/a@1']


def test_does_not_publish_while_playing_in_reverse(make_player):
    timeline = FakeTimeline(FakeBag([entry('/a', 1)]), playhead=1, play_speed=-1.0)
    published = make_player(timeline, ['/a'])
    timeline.view_playhead('/a')
    assert published == []


def test_does_not_publish_message_viewed_while_paused(make_player):
    # Clicking or dragging the playhead, enabling Publish on a topic, and events
    # still queued from reverse playback all reach message_viewed() while paused.
    timeline = FakeTimeline(FakeBag([entry('/a', 1)]), playhead=1)
    published = make_player(timeline, ['/a'])
    timeline.view_playhead('/a')
    assert published == []


def test_step_forward_while_paused_publishes_the_stepped_message_once(make_player):
    timeline = FakeTimeline(FakeBag([entry('/a', 1), entry('/a', 2), entry('/a', 3)]), playhead=1)
    published = make_player(timeline, ['/a'])
    timeline.navigate_next()
    timeline.view_playhead('/a')
    assert published == ['/a@2']


def test_step_forward_while_playing_publishes_the_stepped_message(make_player):
    # navigate_next() stops playback before it moves the playhead.
    timeline = FakeTimeline(FakeBag([entry('/a', 1), entry('/a', 2)]), playhead=1, play_speed=1.0)
    published = make_player(timeline, ['/a'])
    timeline.navigate_next()
    timeline.view_playhead('/a')
    assert published == ['/a@2']


def test_step_back_while_paused_publishes_nothing(make_player):
    timeline = FakeTimeline(FakeBag([entry('/a', 1), entry('/a', 2), entry('/a', 3)]), playhead=3)
    published = make_player(timeline, ['/a'])
    timeline.navigate_previous()
    timeline.view_playhead('/a')
    assert published == []


def test_step_forward_publishes_only_topics_with_a_message_in_the_step(make_player):
    timeline = FakeTimeline(FakeBag([entry('/a', 1), entry('/b', 2), entry('/a', 3)]), playhead=1)
    published = make_player(timeline, ['/a', '/b'])
    timeline.navigate_next()
    assert published == ['/b@2']
    timeline.navigate_next()
    assert published == ['/b@2', '/a@3']


def test_step_forward_from_the_end_wraps_without_publishing(make_player):
    timeline = FakeTimeline(FakeBag([entry('/a', 1), entry('/a', 2)]), playhead=2)
    published = make_player(timeline, ['/a'])
    timeline.navigate_next()
    assert timeline._timeline_frame.playhead == Time(nanoseconds=1)
    assert published == []
