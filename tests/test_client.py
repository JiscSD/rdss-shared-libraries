"""Tests for Kinesis Client """


import json
from .kinesis_helpers import KinesisMixin
from rdsslib.kinesis import client


class TestKinesisClient(KinesisMixin):
    """Test the Kinesis Client."""

    def test_client_writes_and_reads_messages(self, serialised_payload):
        s_client = client.KinesisClient()
        s_client.client.create_stream(StreamName='test_stream', ShardCount=1)
        s_client.write_message(['test_stream'], serialised_payload, 2)
        records = s_client.read_messages('test_stream')
        msg = next(records)
        decoded = json.loads(msg['Data'].decode('utf-8'))
        assert decoded['messageBody'] == {'some': 'message'}
