import boto3
import json
from .kinesis_helpers import KinesisMixin
import pytest
from rdsslib.kinesis import reader


class TestStreamReader(KinesisMixin):
    """Test Kinesis Stream Reader."""

    @pytest.fixture
    def client(self):
        return boto3.client('kinesis')

    def test_read_stream_returns_message(self, serialised_payload, client):
        s_reader = reader.StreamReader(client=client)
        s_reader.client.create_stream(StreamName='test_stream', ShardCount=1)
        s_reader.client.put_record(StreamName='test_stream',
                                   Data=serialised_payload,
                                   PartitionKey='testkey')
        record_gen = s_reader.read_stream('test_stream')
        msg = next(record_gen)
        decoded = json.loads(msg['Data'].decode('utf-8'))
        assert decoded['messageBody'] == {'some': 'message'}
