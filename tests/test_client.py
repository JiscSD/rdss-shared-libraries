"""Tests for Kinesis Client """

import pytest
import moto
from rdsslib.kinesis import client
import json


class TestKinesisClient(object):
    """Test the Kinesis Client."""

    def setup(self):
        """Start mocking kinesis."""
        self.mock = moto.mock_kinesis()
        self.mock.start()

    @pytest.fixture
    def kc(self):
        return client.KinesisClient()

    @pytest.fixture
    def payload(self):
        """Return a sample payload."""
        return {
            'messageHeader': {
                'id': '90cbdf86-6892-4bf9-845f-dbd61eb80065'
            },
            'messageBody': {
                'some': 'message'
            }
        }

    @pytest.fixture
    def serialised_payload(self, payload):
        """Return payload serialised to JSON formatted str"""
        return json.dumps(payload)

    def test_write_and_read_messages(self, serialised_payload, kc):
        kc.client.create_stream(StreamName='test_stream', ShardCount=2)
        kc.write_message(['test_stream'], serialised_payload, 1)
        messages = kc.read_messages('test_stream')
        sample_message = next(messages)
        assert sample_message['SequenceNumber'] == '1'
        assert json.loads(sample_message['Data'].decode('utf-8'))['messageBody'] == {'some' : 'message'}

    def teardown(self):
        """Stop mocking kinesis."""
        self.mock.stop()