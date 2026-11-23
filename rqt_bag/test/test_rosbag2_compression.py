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
import os

import pytest
from rclpy.serialization import serialize_message
from rclpy.time import Time

import rosbag2_py

from rqt_bag.rosbag2 import Rosbag2
from std_msgs.msg import String

TOPIC = '/chatter'
NUM_MESSAGES = 50
BASE_TIME_NS = 1_700_000_000_000_000_000  # 2023-11-14T22:13:20Z


@pytest.fixture
def message_compressed_bag(tmp_path):
    """Create a bag with per-message zstd compression and return its directory."""
    bag_dir = str(tmp_path / 'compressed_bag')
    writer = rosbag2_py.SequentialCompressionWriter(
        rosbag2_py.CompressionOptions(
            compression_format='zstd',
            compression_mode=rosbag2_py.CompressionMode.MESSAGE))
    writer.open(rosbag2_py.StorageOptions(uri=bag_dir, storage_id='mcap'),
                rosbag2_py.ConverterOptions())
    writer.create_topic(rosbag2_py.TopicMetadata(
        id=0, name=TOPIC, type='std_msgs/msg/String', serialization_format='cdr'))
    for i in range(NUM_MESSAGES):
        msg = String()
        msg.data = f'message {i}'
        writer.write(TOPIC, serialize_message(msg), BASE_TIME_NS + i * 1_000_000_000)
    del writer
    gc.collect()
    return bag_dir


def test_compressed_bag_metadata(message_compressed_bag):
    bag = Rosbag2(message_compressed_bag)
    assert bag.metadata.compression_format == 'zstd'
    assert bag.metadata.compression_mode == 'MESSAGE'
    assert bag.get_topics() == [TOPIC]


def test_get_entry_from_compressed_bag(message_compressed_bag):
    bag = Rosbag2(message_compressed_bag)
    mid = Time(nanoseconds=BASE_TIME_NS + NUM_MESSAGES // 2 * 1_000_000_000)
    entry = bag.get_entry(mid, TOPIC)
    assert entry is not None
    assert entry.topic == TOPIC
    assert entry.timestamp == BASE_TIME_NS + NUM_MESSAGES // 2 * 1_000_000_000
    msg, _ = bag.deserialize_entry(entry)
    assert msg.data == f'message {NUM_MESSAGES // 2}'


def test_all_entries_from_compressed_bag(message_compressed_bag):
    bag = Rosbag2(message_compressed_bag)
    entries = list(bag.entries_in_range_generator(
        Time(nanoseconds=BASE_TIME_NS),
        Time(nanoseconds=BASE_TIME_NS + NUM_MESSAGES * 1_000_000_000),
        TOPIC))
    assert len(entries) == NUM_MESSAGES
    for i, entry in enumerate(entries):
        assert entry.timestamp == BASE_TIME_NS + i * 1_000_000_000
        msg, _ = bag.deserialize_entry(entry)
        assert msg.data == f'message {i}'


def test_plain_bag_still_loads(tmp_path):
    """Uncompressed bags must keep working with the plain reader."""
    bag_dir = str(tmp_path / 'plain_bag')
    writer = rosbag2_py.SequentialWriter()
    writer.open(rosbag2_py.StorageOptions(uri=bag_dir, storage_id='mcap'),
                rosbag2_py.ConverterOptions())
    writer.create_topic(rosbag2_py.TopicMetadata(
        id=0, name=TOPIC, type='std_msgs/msg/String', serialization_format='cdr'))
    msg = String()
    msg.data = 'plain'
    writer.write(TOPIC, serialize_message(msg), BASE_TIME_NS)
    del writer
    gc.collect()

    bag = Rosbag2(bag_dir)
    assert os.path.isfile(bag.db_name)
    entry = bag.get_entry(Time(nanoseconds=BASE_TIME_NS), TOPIC)
    assert entry is not None
    deserialized, _ = bag.deserialize_entry(entry)
    assert deserialized.data == 'plain'
