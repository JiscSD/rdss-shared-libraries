from collections import defaultdict
import json
import pytest
import uuid

import moto

from rdsslib.kinesis import factory


class MockStreamWriter(object):
    def __init__(self):
        self.streams = defaultdict(list)

    def put_stream(self, stream_name, payload, max_attempts):
        self.streams[stream_name].append(payload)


class KinesisMixin(object):

    def setup(self):
        self.mock = moto.mock_kinesis()
        self.mock.start()

    @pytest.fixture
    def payload(self):
        """Return a sample payload."""
        return {
            'messageHeader': {
                'id': str(uuid.uuid4())
            },
            'messageBody': {
                'some': 'message'
            }
        }

    @pytest.fixture
    def serialised_payload(self, payload):
        """Return payload serialised to JSON formatted str"""
        return json.dumps(payload)

    def client_works_for_valid_json_messages(self, client_type,
                                             serialised_payload):
        s_client = factory.kinesis_client_factory(client_type)
        s_client.writer.client.create_stream(
            StreamName='test_stream', ShardCount=1)
        s_client.write_message(['test_stream'], serialised_payload, 1)
        records = s_client.read_messages('test_stream')
        msg = next(records)
        decoded = json.loads(msg['Data'].decode('utf-8'))
        assert decoded['messageBody'] == {'some': 'message'}

    def teardown(self):
        """Stop mocking kinesis."""
        self.mock.stop()


def get_records(client, stream_name):
    """Return a list of records from given stream."""
    shard_id = client.describe_stream(
        StreamName=stream_name
    )['StreamDescription']['Shards'][0]['ShardId']
    shard_iterator = client.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType='TRIM_HORIZON'
    )['ShardIterator']
    result = client.get_records(
        ShardIterator=shard_iterator,
        Limit=1000
    )
    return result['Records']
