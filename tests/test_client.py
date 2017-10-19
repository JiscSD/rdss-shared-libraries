"""Tests for Kinesis Client """


import json
from .kinesis_helpers import KinesisMixin, MockStreamWriter
import mock
import pytest
from rdsslib.kinesis import client, decorators, handlers, writer, reader, factory


class TestKinesisClient(KinesisMixin):
    """Test the Kinesis Client."""

    def test_client_writes_and_reads_messages(self, serialised_payload):
        s_client = factory.kinesis_client_factory('figshare')
        s_client.writer.client.create_stream(StreamName='test_stream', ShardCount=1)
        s_client.write_message(['test_stream'], serialised_payload, 2)
        records = s_client.read_messages('test_stream')
        msg = next(records)
        decoded = json.loads(msg['Data'].decode('utf-8'))
        assert decoded['messageBody'] == {'some': 'message'}


class TestEnhancedKinesisClient(KinesisMixin):
    """Test Enhanced Kinesis Client"""

    def test_enhanced_kinesis_client_handles_valid_json(self, serialised_payload):
        test_client = factory.kinesis_client_factory('content_router')
        test_client.writer.client.create_stream(StreamName='test_stream', ShardCount=1)
        test_client.write_message(['test_stream'], serialised_payload, 1)
        msg_generator = test_client.read_messages('test_stream')
        msg = next(msg_generator)
        print(msg)


