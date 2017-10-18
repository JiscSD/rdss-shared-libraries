"""Tests for Kinesis Client """


import json
import moto
import pytest
from rdsslib.kinesis import client


class TestKinesisClient(object):
    """Test the Kinesis Client."""

    def setup(self):
        self.mock = moto.mock_kinesis()
        self.mock.start()

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

    def test_client_writes_and_reads_messages(self, serialised_payload):
        s_client = client.KinesisClient()
        s_client.client.create_stream(StreamName='test_stream', ShardCount=1)
        s_client.write_message(['test_stream'], serialised_payload, 2)
        records = s_client.read_messages('test_stream')
        msg = next(records)
        decoded = json.loads(msg['Data'].decode('utf-8'))
        assert decoded['messageBody'] == {'some': 'message'}

    def teardown(self):
        """Stop mocking kinesis."""
        self.mock.stop()