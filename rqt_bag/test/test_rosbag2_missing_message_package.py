# Copyright 2026 rqt_bag maintainers
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
#    * Neither the name of the rqt_bag maintainers nor the names of its
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


import gc

import pytest
from rclpy.serialization import serialize_message
from rclpy.time import Time

import rosbag2_py

from rqt_bag.message_listener_thread import ListenerEvent
from rqt_bag.plugins.message_view import MessageView
from rqt_bag.rosbag2 import MessageTypeUnavailableError, Rosbag2
from std_msgs.msg import String

# The package of this topic type deliberately does not exist.
TOPIC = '/missing'
MISSING_TYPE = 'missing_pkg/msg/Foo'
BASE_TIME_NS = 1_700_000_000_000_000_000  # 2023-11-14T22:13:20Z


@pytest.fixture
def missing_type_bag(tmp_path):
    """
    Create a bag whose topic type is provided by a package that is not installed.

    Recording such a bag is possible (e.g. when the writer only stores serialized
    payloads), so opening it elsewhere must not break viewing or playback.
    """
    bag_dir = str(tmp_path / 'missing_type_bag')
    writer = rosbag2_py.SequentialWriter()
    writer.open(rosbag2_py.StorageOptions(uri=bag_dir, storage_id='mcap'),
                rosbag2_py.ConverterOptions())
    writer.create_topic(rosbag2_py.TopicMetadata(
        id=0, name=TOPIC, type=MISSING_TYPE, serialization_format='cdr'))
    msg = String()
    msg.data = 'message from a missing package'
    writer.write(TOPIC, serialize_message(msg), BASE_TIME_NS)
    del writer
    gc.collect()
    return bag_dir


class RecordingView(MessageView):
    """A MessageView recording which of the message callbacks was called."""

    def __init__(self, timeline, topic):
        super(RecordingView, self).__init__(timeline, topic)
        self.viewed = []
        self.cleared = 0

    def message_viewed(self, **kwargs):
        self.viewed.append(kwargs)

    def message_cleared(self):
        self.cleared += 1


def get_entry_of_missing_type(bag):
    entry = bag.get_entry(Time(nanoseconds=BASE_TIME_NS), TOPIC)
    assert entry is not None
    return entry


def test_deserialize_entry_raises_message_type_unavailable(missing_type_bag):
    bag = Rosbag2(missing_type_bag)
    entry = get_entry_of_missing_type(bag)

    with pytest.raises(MessageTypeUnavailableError) as excinfo:
        bag.deserialize_entry(entry)
    # The error must name the problem, not leak a bare ModuleNotFoundError.
    assert MISSING_TYPE in str(excinfo.value)
    assert TOPIC in str(excinfo.value)
    assert not isinstance(excinfo.value, ModuleNotFoundError)


def test_message_view_event_handles_unavailable_message_type(missing_type_bag):
    """Displaying such a message must clear the view, not raise in the Qt handler."""
    bag = Rosbag2(missing_type_bag)
    entry = get_entry_of_missing_type(bag)

    view = RecordingView(None, TOPIC)
    view.event(ListenerEvent((bag, entry)))
    assert view.viewed == []
    assert view.cleared == 1
