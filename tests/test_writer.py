import boto3
import json
from .kinesis_helpers import get_records
import logging
import moto
import pytest
from rdsslib.kinesis import writer


class TestStreamWriter(object):
    """Test Kinesis Stream Writer."""

    def setup(self):
        """Start mocking kinesis."""
        self.mock = moto.mock_kinesis()
        self.mock.start()

    @pytest.fixture
    def client(self):
        return boto3.client('kinesis')

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

    def test_put_stream(self, serialised_payload, client):
        logger = logging.getLogger()
        s_writer = writer.StreamWriter(logger)
        s_writer.client.create_stream(StreamName='test_stream', ShardCount=1)
        s_writer.put_stream('test_stream', serialised_payload, 1)
        records = get_records(s_writer.client, 'test_stream')
        for msg in records:
            decoded = json.loads(msg['Data'].decode('utf-8'))
            assert decoded['messageBody'] == {'some': 'message'}

    def teardown(self):
        """Stop mocking kinesis."""
        self.mock.stop()
