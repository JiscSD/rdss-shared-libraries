import boto3
import json
from .kinesis_helpers import KinesisMixin
from moto import (
    mock_kinesis,
)
import pytest
from rdsslib.kinesis import reader
from unittest.mock import (
    patch,
)


class TestStreamReader(KinesisMixin):
    """Test Kinesis Stream Reader."""

    @pytest.fixture
    def client(self):
        return boto3.client('kinesis')

    def test_read_stream_returns_message(self, serialised_payload, client):
        s_reader = reader.StreamReader(client=client, read_interval=0.2)
        s_reader.client.create_stream(StreamName='test_stream', ShardCount=1)
        s_reader.client.put_record(StreamName='test_stream',
                                   Data=serialised_payload,
                                   PartitionKey='testkey')
        record_gen = s_reader.read_stream('test_stream')
        msg = next(record_gen)
        decoded = json.loads(msg['Data'].decode('utf-8'))
        assert decoded['messageBody'] == {'some': 'message'}

    @mock_kinesis
    def test_read_interval_respected(self):
        stream_name = 'test-stream'
        kinesis_client = boto3.client('kinesis', region_name='us-east-1')
        kinesis_client.create_stream(StreamName=stream_name, ShardCount=1)
        kinesis_client.put_record(
            StreamName=stream_name,
            PartitionKey=stream_name,
            Data=json.dumps({'some': 'data'})
        )
        s_reader = reader.StreamReader(client=kinesis_client, read_interval=20)
        records = s_reader.read_stream(stream_name)

        seconds_slept = None

        # The production code is in an infinite loop of sleeps, so we patch
        # sleep to throw an exception to get out of it in the test
        def mock_sleep(seconds):
            nonlocal seconds_slept
            seconds_slept = seconds
            raise Exception()

        next(records)
        with patch('time.sleep', side_effect=mock_sleep):
            try:
                next(records)
            except Exception:
                pass

        assert seconds_slept == 20
