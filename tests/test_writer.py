import boto3
import json
from .kinesis_helpers import get_records, KinesisMixin
import logging
import pytest
from rdsslib.kinesis import writer


class TestStreamWriter(KinesisMixin):
    """Test Kinesis Stream Writer."""

    @pytest.fixture
    def client(self):
        return boto3.client('kinesis')

    def test_put_stream(self, serialised_payload, client):
        logger = logging.getLogger()
        s_writer = writer.StreamWriter(client=client, logger=logger)
        s_writer.client.create_stream(StreamName='test_stream', ShardCount=1)
        s_writer.put_stream('test_stream', serialised_payload, 1)
        records = get_records(s_writer.client, 'test_stream')
        for msg in records:
            decoded = json.loads(msg['Data'].decode('utf-8'))
            assert decoded['messageBody'] == {'some': 'message'}

