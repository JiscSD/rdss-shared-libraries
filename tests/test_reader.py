import boto3
import json
import moto
import pytest
from rdsslib.kinesis import reader


class TestStreamReader(object):
    """Test Kinesis Stream Reader."""

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

    def test_read_stream_returns_message(self, serialised_payload, client):
        s_reader = reader.StreamReader(client=client)
        s_reader.client.create_stream(StreamName='test_stream', ShardCount=1)
        s_reader.client.put_record(StreamName='test_stream',
                                   Data = serialised_payload,
                                   PartitionKey = 'testkey')
        record_gen = s_reader.read_stream('test_stream')
        msg = next(record_gen)
        decoded = json.loads(msg['Data'].decode('utf-8'))
        assert decoded['messageBody'] == {'some': 'message'}

    def teardown(self):
        """Stop mocking kinesis."""
        self.mock.stop()
